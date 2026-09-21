package prewarm

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
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
			return persistentPlaylistFailure(ctx, job, fmt.Sprintf("sprite.vtt not reachable after %d attempts", attempt))
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
		var segmentExtensions []string
		if mediaType == enums.MediaTypeVideo {
			segmentExtensions = []string{".ts", ".mp4", ".m4s", ".jpeg"}
		}
		collected, err := engine.CollectPlaylistURLs(ctx, childURL, segmentExtensions...)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			attempt := prewarmAttempt(job)
			if attempt < maxPlaylistAttempts {
				return fmt.Errorf("playlist not ready (attempt %d/%d): %v: %w", attempt, maxPlaylistAttempts, err, queue.ErrJobRetry)
			}
			return persistentPlaylistFailure(ctx, job, fmt.Sprintf("playlist not reachable after %d attempts: %v", attempt, err))
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

	// จำนวน payload ที่ต้องได้ขนาดครบจริง ๆ คำนวณจาก URL ทั้งชุดก่อนยิง
	// เพื่อไม่ให้ early abort ทำให้ยอดบางส่วนถูกเข้าใจผิดว่าเป็นยอดครบแล้ว
	expectedPayloads := countPayloadURLs(urls)
	var sizedPayloads atomic.Int64
	var payloadBytes atomic.Int64

	stats := engine.Warm(ctx, urls, func(o URLOutcome, done, total int64) {
		if isPayloadURL(o.URL) && o.Err == nil &&
			(o.Status == http.StatusOK || o.Status == http.StatusPartialContent) && o.SizeKnown {
			sizedPayloads.Add(1)
			payloadBytes.Add(o.Size)
		}
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
	failPct := failedPercent(stats)
	if failPct > 50 {
		log.Printf("⚠️ [%s@%s] Warm failed %d/%d URLs (%.0f%%) in %s",
			label, pop, stats.Failed, stats.Total, failPct, took.Round(time.Millisecond))
	} else {
		log.Printf("✅ [%s@%s] Warmed %d URLs (HIT:%d MISS:%d EXPIRED:%d FAILED:%d) in %s",
			label, pop, stats.Total, stats.Hit, stats.Miss, stats.Expired, stats.Failed,
			took.Round(time.Millisecond))
	}

	// เขียนรายการ URL ของ media นี้ลงไฟล์ — ทำหลัง warm จบ ก่อนตัดสินผล
	// เพื่อให้งานที่ fail เกินเกณฑ์ก็ยังมีรายการไว้ไล่ดูว่าพังตรงไหน
	if collect {
		WriteJobLog(JobLogInfo{
			MediaSlug: mediaSlug, FileSlug: fileSlug, Resolution: resLabel,
			Kind: kindLabel, Pop: pop, Took: took,
		}, outcomes, stats)
	}

	// A result with more than half its URLs failed is not a completed prewarm.
	// Keep the queue document pending and do not advance media.prewarmAt. The
	// queue-level circuit breaker pauses this storage after five such results.
	if failPct > 50 {
		return fmt.Errorf("[%s@%s] failed %d/%d urls (%.0f%%): %w",
			label, pop, stats.Failed, stats.Total, failPct, queue.ErrStorageFailure)
	}

	var mediaSize *int64
	if expectedPayloads > 0 && sizedPayloads.Load() == expectedPayloads {
		size := payloadBytes.Load()
		mediaSize = &size
	}
	return recordPrewarm(ctx, job.MediaID, pop, stats, mediaSize)
}

// isPayloadURL แยกไฟล์ข้อมูลจริงออกจาก manifest เพื่อให้ media.size หมายถึง
// ขนาด segment/sprite รวม ไม่รวมไฟล์ควบคุม .m3u8 และ .vtt
func isPayloadURL(rawURL string) bool {
	return !hasReferenceExtension(rawURL, ".m3u8", ".vtt")
}

func countPayloadURLs(urls []string) int64 {
	var total int64
	for _, rawURL := range urls {
		if isPayloadURL(rawURL) {
			total++
		}
	}
	return total
}

func persistentPlaylistFailure(ctx context.Context, job *models.PrewarmQueue, message string) error {
	playable, err := jobSourceIsPlayable(ctx, job.MediaID)
	return classifyPersistentPlaylistFailure(message, playable, err)
}

func classifyPersistentPlaylistFailure(message string, playable bool, verifyErr error) error {
	if verifyErr != nil {
		return fmt.Errorf("%s; source verification failed: %v: %w", message, verifyErr, queue.ErrJobRequeue)
	}
	if !playable {
		// A plain error is intentionally terminal. The queue loop drops this
		// orphan instead of opening the storage circuit for unrelated media.
		return fmt.Errorf("%s; source media or file no longer exists", message)
	}
	return fmt.Errorf("%s: %w", message, queue.ErrStorageFailure)
}

func failedPercent(stats WarmStats) float64 {
	if stats.Total <= 0 {
		return 0
	}
	return float64(stats.Failed) / float64(stats.Total) * 100
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
