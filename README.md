# worker-prewarm

CDN cache prewarmer worker for AVXTube — claims per-media jobs from the
dedicated **`prewarm_queue`** collection, walks the public HLS playlist of
that media, and fires concurrent `HEAD` requests through the CDN
(Cloudflare) so the first real viewer hits a warm edge cache.

```
enqueuer (platform/node-api)               worker-prewarm (this repo)
┌──────────────────────────────┐           ┌─────────────────────────────┐
│ per pop with alive workers:  │  pending  │ Claim (atomic, own pop)     │
│  new: media ยังไม่มี          │ ────────▶ │ video → video.m3u8 +        │
│   prewarm.{pop} (non-fra     │           │   rendition segments        │
│   ต้องรอ fra เสร็จก่อน)       │           │ audio → audio.m3u8 +        │
│  reprewarm: prewarmAt เก่า    │           │   audio segments            │
│   กว่า reprewarm_age_minutes │           │ thumbnail → sprite.vtt +    │
│                                │           │   sprite images             │
└──────────────────────────────┘           │ → medias.prewarm.{pop} แล้ว │
                                           │   ลบ doc ออกจากคิว          │
                                           └─────────────────────────────┘
```

## Design

- **คิวแยก** — งาน prewarm อยู่ใน `prewarm_queue` ไม่ปนกับ `video_process`
  ระหว่างทำงานไม่อัพเดต progress ใดๆ เสร็จแล้วบันทึกผลลง
  `medias.prewarm.{pop} = {data: {total,hit,miss,expired,failed}, prewarmAt}`
  (shape เดิมของระบบเก่า) แล้วลบ doc ทิ้ง — คิวเก็บเฉพาะงานค้างเสมอ
- **Multi-POP** — worker ประกาศ pop ของตัวเอง (`PREWARM_POP`) ผ่าน heartbeat
  และ claim เฉพาะงานของ pop ตัวเอง; enqueuer จัดคิวเฉพาะ pop ที่มี worker
  มีชีวิต งาน new ของ pop อื่นจะยังไม่ถูกจัดจนกว่า **fra** จะ warm media
  ชิ้นนั้นเสร็จก่อน
- **Storage binding** — worker ที่ตั้ง `STORAGE_ID` จะรับงาน **new** เฉพาะ
  media ของ storage ตัวเอง (enqueuer ประทับ `targetStorageId` เมื่อ storage
  นั้นมี worker ผูกอยู่) ส่วนงาน **reprewarm** ไม่ประทับ target — worker
  ไหนก็หยิบได้ ไม่ว่ามี storageId หรือไม่
- playlist/VTT ที่ยังไม่พร้อมจะ retry ทุก 1 นาที; งานที่ URL ล้มเหลวเกิน
  50% จะไม่บันทึก `media.prewarm` และคงอยู่ในสถานะ pending
- **Storage circuit breaker** — ถ้า storage เดียวกันมีงานที่ URL ล้มเหลวเกิน
  50% จำนวน 5 ครั้งภายใน 10 นาที จะพักการ claim storage นั้น 10 นาที
  จากนั้นทดลอง 1 งาน; สำเร็จจึงเปิดตามปกติ หากยังล้มเหลวจะพักต่ออีก 10 นาที

## What gets warmed per job

| Job type | URLs |
|---|---|
| video media | `/{mediaSlug}/video.m3u8` + ทุก segment ของ quality นั้น |
| audio media | `/{mediaSlug}/audio.m3u8` + ทุก audio segment |
| thumbnail media | `/{fileSlug}/sprite/sprite.vtt` + `sprite-*.jpg` ทั้งหมด |

## Settings (MongoDB `settings` collection)

| Name | Shape | Description |
|---|---|---|
| `prewarm` | `{enabled, enabled_old, prewarm_max_concurrent, prewarm_old_max_concurrent, prewarm_parallel, prewarm_old_parallel, reprewarm_age_minutes}` | shape เดิมของ server-prewarm — `enabled`/`enabled_old` เปิดปิดงาน new/reprewarm (ปิดทั้งคู่ = worker หยุด claim), `*_max_concurrent` = งานค้างในคิวต่อ pop แยกชนิด, `*_parallel` = HEAD พร้อมกันต่องานแยกชนิด (default 10/20), `reprewarm_age_minutes` = อายุก่อน warm ซ้ำ (default 60) |
| `domain_setting` | `{domain_content, domain_static, domain_playlist, url_scraping}` | setting ของระบบใหม่: ใช้ `domain_playlist` สำหรับ HLS, `domain_static` สำหรับ sprite และใช้ `domain_content` เป็น Referer |

ระหว่าง rollout worker ยังอ่าน setting แยกชื่อ `domain_playlist`,
`domain_static` และ `domain_preview` เป็น fallback ได้ แต่ค่าหลักมาจาก
`domain_setting` ของ platform ใหม่

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `mongodb://localhost:27017` | MongoDB connection string |
| `PREWARM_POP` | _(auto)_ | edge location ของเครื่องนี้ — ไม่ตั้ง = ตรวจเองจาก CF-Ray ตอน start (ยิง HEAD ไป domain_playlist/storage แล้วอ่าน colo) |
| `STORAGE_ID` | _(empty)_ | ผูกกับ storage — งาน new เฉพาะ media ของ storage นี้ |
| `WORKER_ID` | `prewarm_{hostname}@1` | unique worker id (`prewarm_` prefix required) |
| `PORT` | `8883` | realtime dashboard port |
| `URL_LOG_MODE` | `off` | `off`, `error` หรือ `all` สำหรับ log ราย URL |
| `URL_LOG_DIR` | `logs` | directory สำหรับ `{mediaSlug}.log` |

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/avxtube/worker-prewarm/main/install.sh | sudo -E bash -s -- \
    --database-url "mongodb+srv://user:pass@host/db"
```

### POP อื่น / ผูกกับ storage

```bash
... | sudo -E bash -s -- --database-url "..." --pop sin
... | sudo -E bash -s -- --database-url "..." --storage-id "storage-uuid"
```

### Update binary only (preserve `.env`)

```bash
curl -fsSL https://raw.githubusercontent.com/avxtube/worker-prewarm/main/install.sh | sudo bash -s --
```

### Uninstall

```bash
curl -fsSL https://raw.githubusercontent.com/avxtube/worker-prewarm/main/install.sh | sudo bash -s -- --uninstall
```

## Development

```bash
go build ./...          # build all packages
build.bat               # Windows binary → .build/windows.exe
```

Releases are built by GitHub Actions on `v*` tags (linux amd64 + arm64).

ตรวจสอบเวอร์ชันที่ติดตั้ง:

```bash
/opt/worker-prewarm/worker-prewarm --version
```
