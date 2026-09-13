package prewarm

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"path"

	"worker-prewarm/internal/config"
	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/dashboard"
	"worker-prewarm/internal/db/models"
	"worker-prewarm/internal/queue"
)

// ─── Prewarm pipeline (รายชิ้น media) ──────────────────────────
//
// ข้อมูลงานอ่านจาก queue doc ตรงๆ (slug/mediaSlug/type/quality ถูกก็อป
// มาให้ครบตอน enqueue และ enqueuer ตรวจ file gate มาแล้ว) — ไม่ยิง DB ซ้ำ
// doc เก่าที่ไม่มีฟิลด์พวกนี้ยังมี fallback ไปดึง DB ให้อัตโนมัติ
//
//   video media     → /{mediaSlug}/video.m3u8 + segment ของ rendition นั้น
//                     (1 job = 1 rendition — ไม่แตะ master playlist ระดับไฟล์)
//   audio media     → /{mediaSlug}/audio.m3u8 + segment ของ audio track นั้น
//   thumbnail media → /{fileSlug}/sprite/sprite.vtt + รูป sprite ทั้งหมด
//
// ระหว่างทำงานไม่อัพเดตอะไร — เสร็จแล้วบันทึกผลลง medias.prewarm.{pop}
// playlist ที่ยังไม่พร้อม retry ทุก 1 นาทีสูงสุด 3 ครั้ง ก่อนบันทึก failed
// แล้ว loop ลบ doc ออกจากคิว

const maxPlaylistAttempts = 3

// Run executes one prewarm job. Blocking; respects ctx cancellation.
func Run(ctx context.Context, job *models.PrewarmQueue) error {
	pop := config.AppConfig.Pop

	// ── Settings ──────────────────────────────────────────────
	domainPlaylist := normalizeDomain(getDomainSettingString(ctx, "domain_playlist", enums.SettingDomainPlaylist))
	domainStatic := normalizeDomain(getDomainSettingString(ctx, "domain_static", enums.SettingDomainStatic))
	// domain_content is the public player/content origin in the new platform.
	// Keep domain_preview as a legacy fallback.
	referer := normalizeReferer(getDomainSettingString(ctx, "domain_content", enums.SettingDomainPreview))
	if referer == "" {
		return fmt.Errorf("setting domain_setting.domain_content is not set: %w", queue.ErrJobRequeue)
	}
	kind := "new"
	if job.Kind != nil && *job.Kind != "" {
		kind = *job.Kind
	}
	parallel := prewarmParallel(ctx, kind)

	// ── ข้อมูลงานมาจาก queue doc ตรงๆ ─────────────────────────
	// enqueuer ตรวจ file gate (type video + status ready + ไม่ trash/ลบ) และ
	// ก็อป slug/type/quality มาให้ครบตอน enqueue แล้ว — งานอยู่ในคิวแค่
	// ~1 นาที ไม่ต้องยิง DB ซ้ำเพื่อยืนยันอีก (150 job/นาที × 3 query =
	// ภาระที่ไม่ได้อะไรกลับมา) ถ้าไฟล์เพิ่งถูกลบจริง warm จะได้ 404 →
	// นับเป็น fail → เข้ากติกา retry ตามปกติ ไม่มีอะไรเสียหาย
	jobMeta, err := resolveJobMeta(ctx, job)
	if err != nil {
		return err
	}
	if jobMeta == nil {
		return nil // media/file หายไปแล้ว — ลบงานทิ้งเฉยๆ
	}
	mediaSlug, fileSlug, mediaType := jobMeta.MediaSlug, jobMeta.FileSlug, jobMeta.Type

	engine := NewEngine(parallel, referer)
	start := time.Now()

	// ── Collect URLs (ตาม type ของ media) ─────────────────────
	var urls []string
	label := mediaSlug
	if mediaType == enums.MediaTypeThumbnail {
		if domainStatic == "" {
			// config ไม่พร้อม — ไม่ใช่ความผิดของงาน คืนคิวพร้อมหน่วงเวลา
			return fmt.Errorf("setting domain_static is not set: %w", queue.ErrJobRequeue)
		}
		// sprite map: vtt + รูปทุกใบที่ vtt อ้างถึง
		label = fileSlug + "/sprite"
		urls = engine.CollectVTTURLs(ctx, fmt.Sprintf("%s/%s/sprite/sprite.vtt", domainStatic, fileSlug))
		if len(urls) == 0 {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			attempt := prewarmAttempt(job)
			if attempt < maxPlaylistAttempts {
				return fmt.Errorf("sprite.vtt not ready (attempt %d/%d): %w", attempt, maxPlaylistAttempts, queue.ErrJobRetry)
			}
			log.Printf("⚠️ [%s@%s] sprite.vtt not reachable after %d attempts — recorded as failed", label, pop, attempt)
			return recordPrewarm(ctx, job.MediaID, pop, WarmStats{Total: 1, Failed: 1})
		}
	} else {
		if domainPlaylist == "" {
			return fmt.Errorf("setting domain_playlist is not set: %w", queue.ErrJobRequeue)
		}
		playlistName, err := playlistNameForMediaType(mediaType)
		if err != nil {
			return err
		}
		childURL := fmt.Sprintf("%s/%s/%s", domainPlaylist, mediaSlug, playlistName)
		collected, err := engine.CollectPlaylistURLs(ctx, childURL)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			attempt := prewarmAttempt(job)
			if attempt < maxPlaylistAttempts {
				return fmt.Errorf("playlist not ready (attempt %d/%d): %v: %w", attempt, maxPlaylistAttempts, err, queue.ErrJobRetry)
			}
			log.Printf("⚠️ [%s@%s] playlist not reachable after %d attempts — recorded as failed: %v", label, pop, attempt, err)
			return recordPrewarm(ctx, job.MediaID, pop, WarmStats{Total: 1, Failed: 1})
		}

		// งานคือ media ตัวนี้ตัวเดียว (1 job = 1 rendition) — ไม่แตะ master
		// playlist ระดับไฟล์ เพราะมันเป็นของรวมทุก rendition ถ้า warm ตรงนี้
		// ไฟล์ที่มีหลาย rendition จะถูก warm ซ้ำเท่าจำนวน rendition
		urlSet := map[string]bool{}
		for _, u := range collected {
			urlSet[u] = true
		}
		urls = make([]string, 0, len(urlSet))
		for u := range urlSet {
			urls = append(urls, u)
		}
	}

	// ── Warm (สตรีมผลราย URL ให้ dashboard แบบระบบเก่า) ────────
	hub := dashboard.GetHub()
	resLabel := "sprite"
	if mediaType == enums.MediaTypeAudio {
		resLabel = "audio"
	} else if mediaType != enums.MediaTypeThumbnail && jobMeta.Resolution != "" {
		resLabel = jobMeta.Resolution
	}
	kindLabel := kind
	hub.JobStarted(&dashboard.JobInfo{
		ID: job.ID, MediaSlug: mediaSlug, FileSlug: fileSlug,
		Resolution: resLabel, Kind: kindLabel, Pop: pop,
		Total: int64(len(urls)),
	})

	// สะสมผลราย URL ไว้เขียน log ทีเดียวตอนจบ (เปิดใช้เมื่อ URL log เปิดอยู่)
	var (
		outMu    sync.Mutex
		outcomes []URLOutcome
	)
	collect := urlLogEnabled()
	if collect {
		outcomes = make([]URLOutcome, 0, len(urls))
	}

	stats := engine.Warm(ctx, urls, func(o URLOutcome, done, total int64) {
		if collect {
			outMu.Lock()
			outcomes = append(outcomes, o)
			outMu.Unlock()
		}

		hub.JobProgress(job.ID, done, total)
		errStr := ""
		if o.Err != nil {
			errStr = o.Err.Error()
		}
		hub.Broadcast("url_result", dashboard.URLResult{
			JobID: job.ID, MediaSlug: mediaSlug, FileSlug: fileSlug,
			Resolution: resLabel, URL: path.Base(o.URL),
			Status: o.Status, Cache: o.Cache, Pop: pop,
			Duration: o.Duration.Round(time.Millisecond).String(),
			Error:    errStr, Progress: done, Total: total,
		})
	})
	defer hub.JobDone(job.ID, stats)

	// shutdown/cancel กลางคัน — คืนงานเข้าคิว (สถิติไม่ครบ ไม่บันทึก)
	if ctx.Err() != nil {
		return ctx.Err()
	}

	took := time.Since(start)
	log.Printf("✅ [%s@%s] Warmed %d URLs (HIT:%d MISS:%d EXPIRED:%d FAILED:%d) in %s",
		label, pop, stats.Total, stats.Hit, stats.Miss, stats.Expired, stats.Failed,
		took.Round(time.Millisecond))

	// เขียนรายการ URL ของ media นี้ลงไฟล์ — ทำหลัง warm จบ ก่อนตัดสินผล
	// เพื่อให้งานที่ fail เกินเกณฑ์ก็ยังมีรายการไว้ไล่ดูว่าพังตรงไหน
	if collect {
		WriteJobLog(JobLogInfo{
			MediaSlug: mediaSlug, FileSlug: fileSlug, Resolution: resLabel,
			Kind: kindLabel, Pop: pop, Took: took,
		}, outcomes, stats)
	}

	// หลัง collect playlist สำเร็จ ให้บันทึกผล warm เสมอ ไม่ว่าจะมี segment
	// fail กี่เปอร์เซ็นต์; bounded retry ใช้เฉพาะ playlist/VTT ที่ยังอ่านไม่ได้
	//
	// เหตุผล: งานที่คา nextRetryAt กินโควตาต่อ storage ของ enqueuer ทั้งที่
	// ยังไม่ได้ใช้แบนด์วิดท์อะไรเลย และถ้า worker ของ targetStorageId ดับ
	// งานจะไม่มีใคร claim → retryCount ไม่เพิ่ม → ค้างกินโควตาถาวร
	//
	// พังแล้วบันทึกไปเลยแทน: prewarmAt ถูกตั้งเป็นตอนนี้ media จึงไปเข้าคิว
	// ใหม่เองผ่านช่อง reprewarm เมื่อครบ reprewarm_age_minutes
	if stats.Failed > 0 {
		failPct := float64(stats.Failed) / float64(stats.Total) * 100
		log.Printf("⚠️ [%s@%s] failed %d/%d urls (%.0f%%) — recorded, will retry via reprewarm",
			label, pop, stats.Failed, stats.Total, failPct)
	}

	return recordPrewarm(ctx, job.MediaID, pop, stats)
}

func playlistNameForMediaType(mediaType string) (string, error) {
	switch mediaType {
	case enums.MediaTypeVideo:
		return "video.m3u8", nil
	case enums.MediaTypeAudio:
		return "audio.m3u8", nil
	default:
		return "", fmt.Errorf("unsupported prewarm media type %q", mediaType)
	}
}

func prewarmAttempt(job *models.PrewarmQueue) int {
	if job != nil && job.RetryCount != nil && *job.RetryCount > 0 {
		return *job.RetryCount + 1
	}
	return 1
}
