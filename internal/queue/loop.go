package queue

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ─── Job loop (bi-level slots — แบบ manager ของระบบเก่า) ──────
//
// รันพร้อมกันสองช่องอิสระใน worker ตัวเดียว:
//   new       สูงสุด prewarm_max_concurrent งาน
//   reprewarm สูงสุด prewarm_old_max_concurrent งาน
// เช่น ตั้ง 1/5 → เครื่องนี้ warm พร้อมกันได้ 6 วิดีโอ
// ปิด enabled/enabled_old ช่องไหน ช่องนั้นหยุด claim (ปิดทั้งคู่ = idle)

// JobHandler runs one claimed job. It must respect ctx — on cancel it
// should abort quickly and return ctx's error so the loop Releases the
// job instead of retrying it.
type JobHandler func(ctx context.Context, job *models.PrewarmQueue) error

const (
	claimInterval = 10 * time.Second // idle poll — queue empty / disabled
	busyInterval  = 1 * time.Second  // slot เต็ม — เช็คใหม่เร็วๆ เมื่องานจบ
)

// requeueDelay — งานที่คืนคิวเพราะ config ไม่พร้อม (เช่น domain_playlist
// ยังไม่ตั้ง) รอเท่านี้ก่อนถูกหยิบใหม่ กัน claim-release วนรัว
const requeueDelay = 1 * time.Minute

// RunLoop claims and runs jobs until ctx is cancelled. Blocking — call
// from main after StartHeartbeat is up.
func RunLoop(ctx context.Context, workerID string, handler JobHandler) {
	log.Printf("🔁 Job loop started (poll every %s)", claimInterval)

	// ── Crash recovery: คืนงานค้างของตัวเองเข้าคิวก่อน ─────────
	// (รอบก่อนอาจถือหลายงานพร้อมกัน — คืนหมดแล้วให้วน claim ใหม่เอง)
	if n := releaseOwn(ctx, workerID); n > 0 {
		log.Printf("♻️ Released %d interrupted job(s) back to queue", n)
	}

	var wg sync.WaitGroup
	var activeNew, activeOld int64

	for {
		if ctx.Err() != nil {
			break
		}

		cfg := readPrewarmConfig(ctx)

		claimed := false

		// ── ช่อง new ──────────────────────────────────────────
		if cfg.Enabled && atomic.LoadInt64(&activeNew) < int64(cfg.MaxNew) {
			if claimedJob := tryClaim(ctx, workerID, "new"); claimedJob != nil {
				claimed = true
				atomic.AddInt64(&activeNew, 1)
				wg.Add(1)
				go func(claimed *circuitClaim) {
					defer wg.Done()
					defer atomic.AddInt64(&activeNew, -1)
					runJob(ctx, claimed.job, claimed.lease, handler)
				}(claimedJob)
			}
		}

		// ── ช่อง reprewarm (อิสระจาก new) ─────────────────────
		if cfg.EnabledOld && atomic.LoadInt64(&activeOld) < int64(cfg.MaxOld) {
			if claimedJob := tryClaim(ctx, workerID, "reprewarm"); claimedJob != nil {
				claimed = true
				atomic.AddInt64(&activeOld, 1)
				wg.Add(1)
				go func(claimed *circuitClaim) {
					defer wg.Done()
					defer atomic.AddInt64(&activeOld, -1)
					runJob(ctx, claimed.job, claimed.lease, handler)
				}(claimedJob)
			}
		}

		if claimed {
			continue // ยังมี slot ว่างอาจมีงานต่อ — หยิบทันทีไม่ต้องรอ
		}

		// ไม่ได้งานรอบนี้: slot เต็มอยู่ → เช็คถี่ / คิวว่าง → รอตามปกติ
		slotsFull := (!cfg.Enabled || atomic.LoadInt64(&activeNew) >= int64(cfg.MaxNew)) &&
			(!cfg.EnabledOld || atomic.LoadInt64(&activeOld) >= int64(cfg.MaxOld))
		if slotsFull && (atomic.LoadInt64(&activeNew)+atomic.LoadInt64(&activeOld)) > 0 {
			sleepCtx(ctx, busyInterval)
		} else {
			sleepCtx(ctx, claimInterval)
		}
	}

	// shutdown — รอทุกงานปิดตัว (แต่ละงานเห็น ctx cancel แล้ว Release เอง)
	wg.Wait()
	log.Println("🔁 Job loop stopped")
}

type circuitClaim struct {
	job   *models.PrewarmQueue
	lease storageCircuitLease
}

// tryClaim leaves jobs belonging to an open storage circuit untouched in the
// pending queue. begin also reserves exactly one half-open probe after cooldown.
func tryClaim(ctx context.Context, workerID, kind string) *circuitClaim {
	now := time.Now()
	job, err := Claim(ctx, workerID, kind, prewarmStorageCircuit.blockedStorageIDs(now)...)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("⚠️ Claim(%s) failed: %v", kind, err)
		}
		return nil
	}
	if job == nil {
		return nil
	}
	lease := prewarmStorageCircuit.begin(storageIDForJob(job), now)
	if !lease.allowed {
		// The circuit may have opened between building the Mongo filter and the
		// atomic claim. Put the job back without changing retryCount.
		if releaseErr := Release(ctx, job.ID); releaseErr != nil && ctx.Err() == nil {
			log.Printf("⚠️ Release blocked storage job %s failed: %v", job.ID, releaseErr)
		}
		return nil
	}
	if lease.probe {
		log.Printf("▶️ Storage %s cooldown ended; running probe job %s", lease.storageID, job.ID)
	}
	return &circuitClaim{job: job, lease: lease}
}

// runJob executes one job and settles its final status.
func runJob(ctx context.Context, job *models.PrewarmQueue, lease storageCircuitLease, handler JobHandler) {
	err := handler(ctx, job)
	now := time.Now()
	outcome := circuitNeutral
	if err == nil {
		outcome = circuitSuccess
	} else if errors.Is(err, ErrStorageFailure) {
		outcome = circuitFailure
	}
	openUntil := prewarmStorageCircuit.finish(lease, outcome, now)

	// settle ด้วย ctx ใหม่เสมอ — ตอน shutdown ctx หลักถูก cancel ไปแล้ว
	// แต่เรายังต้องเขียนสถานะปิดงานให้สำเร็จ
	settleCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch {
	case err == nil:
		if e := Complete(settleCtx, job.ID); e != nil {
			log.Printf("⚠️ Complete failed for job %s: %v", job.ID, e)
		}
		if lease.probe {
			log.Printf("▶️ Storage %s probe succeeded; prewarm resumed", lease.storageID)
		}

	case ctx.Err() != nil || errors.Is(err, context.Canceled):
		// shutdown — คืนเข้าคิวทันที ให้ตัวอื่นหยิบต่อ
		if e := Release(settleCtx, job.ID); e != nil {
			log.Printf("⚠️ Release failed for job %s: %v", job.ID, e)
		}
		log.Printf("↩️ Job %s released back to queue: %v", job.ID, err)

	case errors.Is(err, ErrJobRequeue):
		// config ไม่พร้อม (ไม่ใช่ความผิดของงาน) — คืนคิวพร้อมหน่วงเวลา
		if e := ReleaseWithDelay(settleCtx, job.ID, requeueDelay); e != nil {
			log.Printf("⚠️ Release failed for job %s: %v", job.ID, e)
		}
		log.Printf("↩️ Job %s requeued (+%s): %v", job.ID, requeueDelay, err)

	case errors.Is(err, ErrJobRetry):
		if e := RetryWithDelay(settleCtx, job.ID, requeueDelay); e != nil {
			log.Printf("⚠️ Retry failed for job %s: %v", job.ID, e)
		}
		log.Printf("🔄 Job %s retrying (+%s): %v", job.ID, requeueDelay, err)

	case errors.Is(err, ErrStorageFailure):
		delay := requeueDelay
		if remaining := time.Until(openUntil); remaining > delay {
			delay = remaining
		}
		if !openUntil.IsZero() && lease.storageID != "" {
			if e := deferPendingStorageJobs(settleCtx, lease.storageID, job.Pop, openUntil); e != nil {
				log.Printf("⚠️ Defer pending jobs for storage %s failed: %v", lease.storageID, e)
			}
		}
		if e := ReleaseWithDelay(settleCtx, job.ID, delay); e != nil {
			log.Printf("⚠️ Release failed for storage job %s: %v", job.ID, e)
		}
		if !openUntil.IsZero() {
			log.Printf("⏸️ Storage %s paused until %s after %d failures above 50%% within %s",
				lease.storageID, openUntil.Format(time.RFC3339), storageFailureThreshold, storageFailureWindow)
		}
		log.Printf("↩️ Job %s kept pending (+%s), prewarm result not recorded: %v", job.ID, delay.Round(time.Second), err)

	default:
		// ไม่ retry ในคิว — ทิ้งงานไปเลย เพราะ enqueuer ตรวจทุกนาทีอยู่แล้ว
		// media ที่ยังไม่มีผล warm จะถูกจัดเข้าคิวใหม่เอง ส่วนตัวที่บันทึกผล
		// แล้วก็รอรอบ reprewarm ตามอายุ — งานที่คา nextRetryAt มีแต่จะกิน
		// โควตาของ enqueuer ทิ้งไว้เฉยๆ (ยิ่งถ้า worker เจ้าของ storage ดับ
		// จะไม่มีใคร claim ให้ retry ค้างถาวร)
		if e := Complete(settleCtx, job.ID); e != nil {
			log.Printf("⚠️ Drop failed for job %s: %v", job.ID, e)
		}
		log.Printf("❌ Job %s dropped (จะกลับมาตามรอบ enqueue ใหม่): %v", job.ID, err)
	}
}

// deferPendingStorageJobs persists the cooldown on existing queue documents.
// This prevents an immediate retry after a worker restart and makes sibling
// workers skip the same storage until the cooldown expires.
func deferPendingStorageJobs(ctx context.Context, storageID, pop string, until time.Time) error {
	filter := bson.M{
		"status": "pending",
		"pop":    pop,
		"$or": []bson.M{
			{"storageId": storageID},
			{"targetStorageId": storageID},
		},
	}
	_, err := models.PrewarmQueueModel.Col().UpdateMany(ctx, filter, bson.M{
		"$max": bson.M{"nextRetryAt": until},
	})
	return err
}

// releaseOwn คืนงาน processing ทั้งหมดของ worker นี้กลับเป็น pending
// (เรียกตอน start — งานค้างจาก crash/restart)
func releaseOwn(ctx context.Context, workerID string) int64 {
	res, err := models.PrewarmQueueModel.Col().UpdateMany(ctx,
		bson.M{"status": "processing", "workerId": workerID},
		bson.M{
			"$set":   bson.M{"status": "pending"},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		},
	)
	if err != nil {
		log.Printf("⚠️ releaseOwn failed: %v", err)
		return 0
	}
	return res.ModifiedCount
}

// ReleaseWithDelay คืนงานเข้าคิวแบบมี nextRetryAt — ไม่นับ retry
func ReleaseWithDelay(ctx context.Context, jobID string, d time.Duration) error {
	_, err := models.PrewarmQueueModel.FindOneAndUpdate(ctx,
		bson.M{"_id": jobID, "status": "processing"},
		bson.M{
			"$set":   bson.M{"status": "pending", "nextRetryAt": time.Now().Add(d)},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		},
	)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	return err
}

// RetryWithDelay returns a transiently failed job to pending and increments
// retryCount. Run uses the persisted count to cap playlist readiness retries.
func RetryWithDelay(ctx context.Context, jobID string, d time.Duration) error {
	_, err := models.PrewarmQueueModel.FindOneAndUpdate(ctx,
		bson.M{"_id": jobID, "status": "processing"},
		bson.M{
			"$set":   bson.M{"status": "pending", "nextRetryAt": time.Now().Add(d)},
			"$inc":   bson.M{"retryCount": 1},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		},
	)
	return err
}

// ─── Settings ─────────────────────────────────────────────────

// PrewarmConfig คือค่าที่ loop ใช้ตัดสินจำนวนงานพร้อมกันต่อช่อง
type PrewarmConfig struct {
	Enabled    bool
	EnabledOld bool
	MaxNew     int
	MaxOld     int
}

var defaultPrewarmConfig = PrewarmConfig{
	Enabled:    true,
	EnabledOld: true,
	MaxNew:     1,
	MaxOld:     5,
}

// settingsPollInterval — อ่าน setting จาก DB ถี่สุดเท่านี้ (โดน loop เรียกทุกรอบ)
const settingsPollInterval = 10 * time.Second

var (
	cachedCfg   = defaultPrewarmConfig
	cachedCfgAt time.Time
	cachedCfgMu sync.Mutex
)

// readPrewarmConfig อ่าน setting "prewarm" (cache 10 วิ) — missing/malformed
// = default (fail-open: a broken settings doc must not stop every worker)
func readPrewarmConfig(ctx context.Context) PrewarmConfig {
	cachedCfgMu.Lock()
	if time.Since(cachedCfgAt) < settingsPollInterval {
		cfg := cachedCfg
		cachedCfgMu.Unlock()
		return cfg
	}
	cachedCfgMu.Unlock()

	cfg := defaultPrewarmConfig
	setting, err := models.SettingModel.FindOne(ctx, bson.M{"name": enums.SettingPrewarm})
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) && ctx.Err() == nil {
			log.Printf("⚠️ Read prewarm setting failed: %v", err)
		}
	} else if m, ok := asBsonM(setting.Value); ok {
		if v, ok := m["enabled"].(bool); ok {
			cfg.Enabled = v
		}
		if v, ok := m["enabled_old"].(bool); ok {
			cfg.EnabledOld = v
		}
		if n := toInt(m["prewarm_max_concurrent"]); n > 0 {
			cfg.MaxNew = n
		}
		if n := toInt(m["prewarm_old_max_concurrent"]); n > 0 {
			cfg.MaxOld = n
		}
	}

	cachedCfgMu.Lock()
	cachedCfg, cachedCfgAt = cfg, time.Now()
	cachedCfgMu.Unlock()
	return cfg
}

// MaxJobs คืนจำนวนงานพร้อมกันสูงสุดตอนนี้ (heartbeat รายงานให้ admin)
func MaxJobs(ctx context.Context) int {
	cfg := readPrewarmConfig(ctx)
	total := 0
	if cfg.Enabled {
		total += cfg.MaxNew
	}
	if cfg.EnabledOld {
		total += cfg.MaxOld
	}
	if total == 0 {
		total = 1
	}
	return total
}

// asBsonM แปลงค่า interface{} ที่อาจ decode มาเป็น bson.M / map / bson.D
func asBsonM(v interface{}) (bson.M, bool) {
	switch m := v.(type) {
	case bson.M:
		return m, true
	case map[string]interface{}:
		return bson.M(m), true
	case bson.D:
		// default registry decode document เป็น bson.D ไม่ใช่ bson.M
		out := bson.M{}
		for _, e := range m {
			out[e.Key] = e.Value
		}
		return out, true
	default:
		return nil, false
	}
}

func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	}
	return 0
}

// sleepCtx sleeps for d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
