# SE Report

ระบบเว็บแอปพลิเคชันสำหรับดึงและแสดงผลรายงานเซอร์เวย์ (Survey/Claim Report) จากระบบ [iSurvey](https://cloud.isurvey.mobi) พร้อม Pivot และ Spreadsheet view

## Tech Stack

| Layer     | Technology                                |
| --------- | ----------------------------------------- |
| Backend   | Python 3.13 / Flask                       |
| Frontend  | Vanilla HTML + CSS + JavaScript           |
| Charts    | Plotly.js (ผ่าน PivotTable renderers)     |
| Pivot     | PivotTable.js + jQuery                    |
| Spreadsheet | Univer (`@univerjs/presets` UMD via unpkg, lazy-loaded) |
| Export    | SheetJS (xlsx)                             |
| Deploy    | Docker + Gunicorn                         |
| API       | iSurvey REST API                          |

## Features

### Data Fetching
- เชื่อมต่อ iSurvey API ดึงข้อมูลรายงานแบบ pagination อัตโนมัติ
- รองรับ 2 ประเภทรายงาน: **Enquiry** (รายงานเซอร์เวย์) และ **Close Claim** (ปิดเคลม)
- เลือกช่วงวันที่ (date range) ได้ สูงสุด 2 ปี
- **Parallel chunk fetching** — ซอยช่วงวันที่เป็น chunk ละ 30 วัน แล้ว fetch พร้อมกัน 4 workers (ThreadPoolExecutor) ผ่าน `queue.Queue` ส่ง event กลับ main generator; `threading.Lock` double-check pattern ใน `login()` ป้องกัน race condition
- **Large page size** — `PAGE_LIMIT=5000` ต่อ request ลด HTTP round-trip
- **SSE Streaming** — แสดง progress bar real-time ระหว่างดึงข้อมูล พร้อมปุ่ม Cancel
- **Auto retry** — retry อัตโนมัติ 3 ครั้งเมื่อเจอ server error (502/503/504)
- **Session refresh** — re-login อัตโนมัติเมื่อ session หมดอายุระหว่างดึงข้อมูล

### Table View
- ตารางแสดงข้อมูลพร้อม column filter (กรองข้อมูลรายคอลัมน์)
- Sidebar เลือกแสดง/ซ่อนคอลัมน์ (Select All / Deselect All)
- ค้นหาค่าใน filter dropdown ได้
- **Virtual scrolling** — render เฉพาะ rows ใน viewport (+buffer) ใช้ persistent top/bottom spacer เป็น anchor เพื่อให้ `scrollHeight` คงที่; DOM คงที่ ~80 rows แม้ dataset 100k+ records

### Pivot View
- PivotTable.js พร้อม drag & drop fields
- รองรับ chart renderers ผ่าน Plotly.js (Bar, Line, Area, Scatter, Pie ฯลฯ)
- Aggregators: Count, Sum, Average ฯลฯ
- Natural sort (numeric-aware) ของ field values

### Spreadsheet View
- ฝัง [Univer](https://univer.ai) ผ่าน CDN (unpkg UMD) แบบ **lazy-load** — โหลดเฉพาะเมื่อกดเข้า tab ครั้งแรก (~2 MB ครั้งเดียว, ไม่กระทบ initial load ของ user ที่ไม่ใช้)
- Auto-populate จากข้อมูลที่ filter ใน Table view ปัจจุบัน
- Features: Core sheet + formulas (`=SUM`, `=AVERAGE`, ฯลฯ) + Filter + Sort + Find & Replace + Conditional Formatting (ผ่าน `@univerjs/preset-sheets-*`)
- **Persistence ที่เครื่อง user** — Save → ดาวน์โหลด `.univer.json` / Open → file picker (ไม่ใช้ backend storage)
- **Save as XLSX** — บันทึกเป็น .xlsx ผ่าน SheetJS (สูตรกลายเป็นค่า — lossy; ใช้ Save .univer.json ถ้าต้องกลับมาแก้)
- Dirty tracking + `beforeunload` warning เตือนก่อนออกถ้ายังไม่ save
- Performance gate ที่ 50k rows — confirm dialog ก่อน mount dataset ใหญ่
- **Theme-immune (always-light)** — popovers/filter dropdowns ของ Univer ไม่ถูก dark theme บัง

### Export
- ปุ่ม Export Excel ดาวน์โหลดข้อมูลที่กรองแล้วเป็นไฟล์ .xlsx
- คอลัมน์เลขเคลมแสดงเป็น text (ไม่แปลงเป็นตัวเลข)
- ปรับความกว้างคอลัมน์อัตโนมัติ

### UI Preferences
- **Theme toggle** — สลับธีมสว่าง/มืด จำค่าใน localStorage
- **Font size** — ปรับขนาดตัวอักษร (A-/A+) ใช้กับตาราง, sidebar, pivot จำค่าใน localStorage
- **Persistent column preferences** — จำคอลัมน์ที่เลือกแสดง/ซ่อนไว้แยกตามประเภทรายงาน

### Responsive Design
- รองรับหน้าจอมือถือ — Sidebar overlay, Toolbar จัดเรียงอัตโนมัติ, ตาราง scroll แนวนอน

### Security
- Basic Authentication (optional) ผ่าน environment variables

## Performance

ทดสอบกับ iSurvey (report type: `enquiry`, 49 คอลัมน์):

| ช่วง | Records | เวลา fetch | Browser memory |
| ---- | ------- | ---------- | -------------- |
| 60d  | 17,706  | 48s        | ~220 MB        |
| 240d | 30,565  | 1:49 min   | ~280 MB        |
| 365d | 103,540 | 3:53 min   | **465 MB**     |

**Tuning constants** (ใน `app.py`):

```python
APP_VERSION = '1.5.0'                          # แสดงที่ login page footer

CHUNK_DAYS = 30                                # ซอยช่วงวันที่เป็นก้อนละ 30 วัน
PAGE_LIMIT = 5000                              # records ต่อ 1 request ของ iSurvey
MAX_WORKERS = 4                                # concurrent chunks ต่อ user (default; ใต้ per-account limit ~8 ของ iSurvey)
FAST_MAX_WORKERS = 6                           # override สำหรับ user ใน FAST_MODE_USERS
FAST_MODE_USERS = {'noppadol', 'noppadols'}    # whitelist รับ FAST_MAX_WORKERS — heavy users (ดึง 1 ปี)
ISURVEY_MAX_CONCURRENT = 30                    # process-wide semaphore cap iSurvey HTTP calls
REQUEST_TIMEOUT = 120                          # วินาทีต่อ request
```

Heavy-user fetch (1 ปี / 365d) ที่ใช้ `FAST_MAX_WORKERS=6` คาดเร็วกว่า baseline ~33% (3:53 min → ~2:35 min)

## Project Structure

```
se-report/
├── app.py              # Flask backend + iSurvey API client + SSE streaming
├── templates/
│   └── index.html      # Frontend (Table + Pivot + Spreadsheet + Filters)
├── requirements.txt    # Python dependencies
├── Dockerfile          # Docker image build (Gunicorn timeout 600s)
├── .dockerignore
├── .gitignore
└── .env                # Environment variables (not tracked)
```

## Setup

### 1. Environment Variables

สร้างไฟล์ `.env`:

```env
SECRET_KEY=<random_32+_char_string>

# Optional — เปิดเฉพาะกรณีจำเป็น
# FLASK_ENV=production            # บังคับให้ raise ถ้า SECRET_KEY หาย (กัน misconfig เงียบ)
# SESSION_COOKIE_SECURE=false     # ปิดเฉพาะ local dev บน HTTP เท่านั้น (default = true)
# LOG_LEVEL=INFO                  # DEBUG / INFO / WARNING / ERROR
# PORT=5000                       # พอร์ตของ Flask dev server
```

สุ่ม `SECRET_KEY` ได้ด้วย:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

> **Cookie security:** `SESSION_COOKIE_SECURE=true` เป็น default แปลว่า cookie
> จะถูกส่งเฉพาะบน HTTPS เท่านั้น (เหมาะกับ production ที่ deploy ผ่าน
> Dokploy/Traefik ซึ่งหน้า proxy ทำ TLS ให้) ถ้าเทสที่ local ผ่าน HTTP
> ต้องตั้ง `SESSION_COOKIE_SECURE=false` ใน `.env` ไม่เช่นนั้น browser จะ
> ไม่ส่ง cookie กลับมาและ login ไม่ติด

> ระบบไม่มี user database ของตัวเอง — login ด้วย username/password ของ
> **iSurvey โดยตรง** เซิร์ฟเวอร์เก็บ credentials ของแต่ละ user ไว้ใน
> หน่วยความจำเท่านั้น (ไม่บันทึกลงไฟล์/cookie ของเบราว์เซอร์) จึงต้อง
> login ใหม่ทุกครั้งที่ restart Flask process

### 2. Run Locally

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

เปิดเบราว์เซอร์ไปที่ `http://localhost:5000` → จะ redirect ไปหน้า
**`/login`** กรอก username/password ของ iSurvey ที่ใช้งานอยู่แล้ว เพื่อ
เข้าใช้ระบบ

> บน macOS พอร์ต 5000 มักชนกับ AirPlay Receiver สามารถกำหนดพอร์ตอื่นผ่าน env var ได้:
> ```bash
> PORT=5050 python app.py
> ```

### 3. Run with Docker

```bash
docker build -t se-report .
docker run -p 5000:5000 --env-file .env se-report
```

> **Gunicorn / multi-worker:** session registry เป็น in-memory dict
> ฝังใน Flask process เดียว — ถ้า deploy ด้วย Gunicorn หลาย worker
> ต้องเปลี่ยนไปใช้ `Flask-Session` (filesystem หรือ Redis backend) ก่อน
> มิฉะนั้น user จะถูกเด้งกลับหน้า login ถ้า request หลังจาก login
> ถูก route ไปอีก worker หนึ่ง

## Progress

- [x] Flask backend + iSurvey API client (login, fetch all pages)
- [x] Frontend table view พร้อม column toggle
- [x] Column filter (search + checkbox per column)
- [x] Dashboard view (summary cards + charts)
- [x] รองรับ 2 ประเภทรายงาน (Enquiry / Close Claim)
- [x] Basic Authentication
- [x] Docker support
- [x] Export ข้อมูลเป็น Excel (.xlsx) พร้อม text format สำหรับเลขเคลม
- [x] Persistent column preferences (จำคอลัมน์ที่เลือกไว้ใน localStorage)
- [x] Responsive design สำหรับ mobile
- [x] Pivot view (PivotTable.js + Plotly chart renderers)
- [x] SSE streaming + progress bar + cancel button
- [x] Auto retry + session refresh สำหรับดึงข้อมูลช่วงยาว
- [x] Gunicorn timeout 600s สำหรับ Docker
- [x] Theme toggle (สว่าง/มืด)
- [x] ปรับขนาดตัวอักษร (A-/A+)
- [x] Migrate dashboard charts จาก ApexCharts → Chart.js v4 (+ chartjs-chart-treemap plugin)
- [x] เปลี่ยน chart "สถานะงาน" จาก Donut เป็น Bar แนวนอน
- [x] เพิ่ม chartjs-plugin-datalabels แสดงตัวเลขปลายแท่งใน chart "สถานะงาน" และ "ผู้ตรวจสอบงาน"
- [x] เปลี่ยน chart "ประเภทเคลม" (Donut) เป็น "ผู้ตรวจสอบงาน" (Bar) พร้อม data label
- [x] Auto re-login เมื่อ iSurvey session หมดอายุระหว่างดึงข้อมูล (จับ JSON parse error)
- [x] ปรับ Dashboard layout เป็น fit-to-viewport (flex grid) ให้เห็นทั้งหน้าโดยไม่ต้อง scroll/zoom
- [x] Parallel chunk fetching (ThreadPoolExecutor 4 workers + queue.Queue + threading.Lock double-check login)
- [x] ขยาย `PAGE_LIMIT` 200 → 5000 และเพิ่ม `CHUNK_DAYS=30`, `HTTPAdapter pool_maxsize`
- [x] Virtual scrolling สำหรับ table view (persistent spacer anchors) — รองรับ 100k+ records, memory peak < 500 MB
- [x] Per-user iSurvey login (เลิกใช้ shared service account; credentials เก็บใน-memory ต่อ user)
- [x] Collapse Dashboard เป็น single full-viewport treemap (พนักงานตรวจสอบ / ผู้ปิดงาน) — labels scale ตาม cell dimensions
- [x] Spreadsheet view ด้วย FortuneSheet (lazy-load + .fsheet.json persistence)
- [x] Migrate Spreadsheet engine จาก FortuneSheet → **Univer** — รองรับ filter / sort / find-replace / conditional formatting, theme-immune popovers, ไฟล์ persistence เปลี่ยนเป็น .univer.json
- [x] Drop pivot column-sort feature (PivotTable.js sort เป็น global per-axis ไม่ใช่ per-column ทำให้สับสนกับ user)
- [x] ลบ Dashboard view ออกจากโครงการ (รวมถึง Chart.js / chartjs-chart-treemap / chartjs-plugin-datalabels และ helpers ทั้งหมด) — เหลือแค่ Table / Pivot / Spreadsheet
- [x] ซ่อน radio Enquiry / Close Claim ใน toolbar (`display:none`) — คง default `enquiry` ไว้สำหรับ form submit
- [x] Gunicorn `gthread` workers (1 worker × 48 threads) แทน sync — รองรับ SSE หลาย stream พร้อมกัน, แก้ปัญหา 30 user คนแรก fetch แล้วคนอื่นรอ
- [x] Process-wide semaphore (`ISURVEY_MAX_CONCURRENT=30`) wrap iSurvey HTTP calls — cap concurrent connections จากระบบเรากัน iSurvey rate-limit
- [x] Per-user fast mode — `FAST_MODE_USERS={'noppadol','noppadols'}` ได้ `MAX_WORKERS=6` (จาก default 4) ทำให้ดึง 1 ปี เร็วขึ้น ~33%; user อื่นไม่กระทบ
- [x] `APP_VERSION` แสดงที่ login page footer (มุมขวาล่าง) ผ่าน Jinja context processor — admin ตรวจ build live ได้โดยไม่ต้องเช็ค server
- [x] **Security hardening** — CSRF token ต่อ session (validate ทุก POST ยกเว้น `/login`), `SESSION_COOKIE_SECURE/HttpOnly/SameSite=Lax`, raise loudly ถ้า `SECRET_KEY` หายใน `FLASK_ENV=production`
- [x] **Memory leak fix** — `_USER_CLIENTS` sweep ทุก 10 นาที ตัด sid ที่ idle เกิน session lifetime (8 ชม.) แทนที่จะค้างใน RAM จน restart
- [x] **Server-side logging** — `logging.basicConfig` + `log.warning/exception` ทุก error path (login fail, CSRF reject, fetch_stream abort) — debug production ได้
- [x] **Retry budget ลด** — `Retry(total=2, backoff_factor=0.5)` แทน `3 / 1.0` กันค้างยาวเกิน 9-min deadline
- [x] **HTTP pool ขยาย** — `pool_maxsize=FAST_MAX_WORKERS*2` กัน urllib3 "pool is full" warning สำหรับ fast-mode users
- [x] **Progress UX** — extrapolate per-chunk avg ไปทุก chunk แทน sum-of-reported เปลี่ยน label เป็น `Chunk X/Y — A/B records (P%)` (pct ไม่กระตุก/ถอย)
- [x] **Cleanup** — ลบ `/fetch` sync route + `fetch_all_pages` ที่ frontend ไม่ใช้แล้ว, ลบการแก้ `_logged_in` จากนอก class
- [x] **orjson** แทน stdlib json — encode SSE done event (full dataset 30-50 MB) เร็วขึ้น 5-10x, drop-in ผ่าน `_jdumps` helper
- [x] **Flask-Compress** — gzip/zstd/br/deflate ทุก response รวม `text/event-stream` (streaming อยู่); index.html 153KB → 34KB (-77%), done event ลด wire payload ~80%
- [x] **Lazy-load SheetJS** (~1.4 MB) — โหลดเฉพาะตอนกด Export / Save XLSX / Open .xlsx; first paint ของ table view เร็วขึ้นทันที
- [x] **Excel import (.xlsx / .xls)** — ปุ่ม Open รับทั้ง `.univer.json` และ Excel; แปลง SheetJS workbook → Univer (values + formulas + merged cells; styles หายเพราะ SheetJS community ไม่อ่าน)
- [x] **Fast-mode user icon** — username ใน `FAST_MODE_USERS` แสดงไอคอน ⚡ สีอำพันแทนรูปคน
- [x] **Row-coherent Sort** — intercept Univer's `sort-range-asc/desc` แล้วเรียงทั้ง data region โดยใช้คอลัมน์ที่เลือกเป็น sort key (Univer's `-ext` auto-expand fail กับ sparse cellData ของเรา → row integrity แตก)
- [x] **Sheet ribbon ปรับให้ flat ขึ้น** — ย้าย Insert (image/hyperlink) + Data tools (filter/sort/find/table/dv/cf/text-to-number) มาที่ Start tab + ซ่อน Insert/Data tabs; tab labels เปลี่ยนเป็นไทย (`เมนู / สูตร / โฟกัส`)
- [x] **Empty state Open button** — ปุ่ม Open ใน Sheet view โผล่ตั้งแต่ยังไม่มี data (ก่อนหน้านี้ปุ่มอยู่ใน Univer ribbon ที่ render ก็ต่อเมื่อ workbook ถูก mount)
- [x] **xlsx Excel Tables import** — lazy-load JSZip + parse `xl/tables/*.xml` + `xl/worksheets/_rels/sheet{N}.xml.rels` → SHEET_TABLE_PLUGIN resource → Univer's table preset render ครบ (filter dropdown + alternating stripes)
- [x] **xlsx cell styles import** — walk `xl/styles.xml` + `xl/worksheets/sheet*.xml` เอง (SheetJS CE ทิ้ง `s` indices) ครอบคลุม **borders (style + color)**, **fills**, **fonts (bold/italic/underline/strike/color/size/family)**, **alignment (horizontal/vertical/wrap)**; border enum verify จาก `window.UniverCore.BorderStyleTypes` runtime
