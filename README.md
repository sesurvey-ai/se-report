# SE Report

ระบบเว็บแอปพลิเคชันสำหรับดึงและแสดงผลรายงานเซอร์เวย์ (Survey/Claim Report) จากระบบ [iSurvey](https://cloud.isurvey.mobi) พร้อม Dashboard สรุปข้อมูลเชิงสถิติ

## Tech Stack

| Layer     | Technology                                |
| --------- | ----------------------------------------- |
| Backend   | Python 3.13 / Flask                       |
| Frontend  | Vanilla HTML + CSS + JavaScript           |
| Charts    | Chart.js v4 (+ chartjs-chart-treemap, chartjs-plugin-datalabels) + Plotly.js |
| Pivot     | PivotTable.js + jQuery                    |
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

### Dashboard View
- **Summary Cards** — จำนวนเคลมทั้งหมด, เสร็จแล้ว, รอดำเนินการ, เวลาเดินทางเฉลี่ย
- **Bar Charts** — สถานะงาน (พร้อม data label), ผู้ตรวจสอบงาน (พร้อม data label), จังหวัดที่เกิดเหตุ, ศูนย์
- **Donut Chart** — เขตพื้นที่
- **Treemap** — พนักงานตรวจสอบ
- **Fit-to-viewport layout** — flex grid 2×3 ปรับขนาด chart อัตโนมัติให้เห็นทั้ง dashboard ในหน้าจอเดียวโดยไม่ต้อง zoom out
- Dashboard สะท้อน column filter ที่ตั้งไว้แบบ real-time
- Chart.js repaint อัตโนมัติเมื่อสลับธีมสว่าง/มืด

### Pivot View
- PivotTable.js พร้อม drag & drop fields
- รองรับ chart renderers ผ่าน Plotly.js (Bar, Line, Area, Scatter, Pie ฯลฯ)
- Aggregators: Count, Sum, Average ฯลฯ

### Export
- ปุ่ม Export Excel ดาวน์โหลดข้อมูลที่กรองแล้วเป็นไฟล์ .xlsx
- คอลัมน์เลขเคลมแสดงเป็น text (ไม่แปลงเป็นตัวเลข)
- ปรับความกว้างคอลัมน์อัตโนมัติ

### UI Preferences
- **Theme toggle** — สลับธีมสว่าง/มืด จำค่าใน localStorage
- **Font size** — ปรับขนาดตัวอักษร (A-/A+) ใช้กับตาราง, sidebar, pivot จำค่าใน localStorage
- **Persistent column preferences** — จำคอลัมน์ที่เลือกแสดง/ซ่อนไว้แยกตามประเภทรายงาน

### Responsive Design
- รองรับหน้าจอมือถือ — Sidebar overlay, Toolbar จัดเรียงอัตโนมัติ, ตาราง scroll แนวนอน, Dashboard 1 คอลัมน์

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
CHUNK_DAYS = 30         # ซอยช่วงวันที่เป็นก้อนละ 30 วัน
PAGE_LIMIT = 5000       # records ต่อ 1 request ของ iSurvey
MAX_WORKERS = 4         # concurrent chunks (iSurvey rate-limit ที่ 8 workers ล้ม)
REQUEST_TIMEOUT = 120   # วินาทีต่อ request
```

## Project Structure

```
se-report/
├── app.py              # Flask backend + iSurvey API client + SSE streaming
├── templates/
│   └── index.html      # Frontend (UI + Charts + Pivot + Filters)
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
```

สุ่ม `SECRET_KEY` ได้ด้วย:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

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
