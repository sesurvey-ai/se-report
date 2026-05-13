import concurrent.futures
import json
import os
import queue
import threading
import time
from datetime import datetime, timedelta
from functools import wraps

import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

# Tuning constants for parallel chunk fetching.
# iSurvey rate-limits concurrent connections; 8 workers failed in prior tests.
CHUNK_DAYS = 30
PAGE_LIMIT = 5000
MAX_WORKERS = 4
REQUEST_TIMEOUT = 120


def check_basic_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_user = os.getenv('AUTH_USER')
        auth_pass = os.getenv('AUTH_PASS')
        if not auth_user or not auth_pass:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or auth.username != auth_user or auth.password != auth_pass:
            return Response(
                'Login required.', 401,
                {'WWW-Authenticate': 'Basic realm="SE Report"'},
            )
        return f(*args, **kwargs)
    return decorated

BASE_URL = 'https://cloud.isurvey.mobi/web/php'


class ISurveyClient:
    def __init__(self):
        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=MAX_WORKERS * 2,
            pool_maxsize=MAX_WORKERS * 2,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self._logged_in = False
        self._login_lock = threading.Lock()

    def login(self):
        # Double-check pattern: avoid stampede when multiple worker threads
        # hit an expired session and try to re-login simultaneously.
        if self._logged_in:
            return
        with self._login_lock:
            if self._logged_in:
                return
            user = os.getenv('ISURVEY_USER')
            password = os.getenv('ISURVEY_PASS')
            if not user or not password:
                raise RuntimeError(
                    'ISURVEY_USER / ISURVEY_PASS ไม่ได้ตั้งค่าใน .env'
                )
            res = self.session.post(
                f'{BASE_URL}/login.php',
                data={'username': user, 'password': password},
                timeout=15,
            )
            res.raise_for_status()
            # iSurvey returns 200 even on bad credentials — verify by looking
            # for the login form in the response body.
            body_lower = res.text.lower()
            if '<form' in body_lower and 'password' in body_lower:
                raise RuntimeError(
                    'iSurvey login ล้มเหลว — ตรวจสอบ ISURVEY_USER / ISURVEY_PASS'
                )
            self._logged_in = True

    def get_report_page(self, params, timeout=60):
        """Fetch a single report page. Auto re-login once on 401/403 or invalid
        JSON (which usually means the session expired and iSurvey returned an
        HTML login page instead of the expected JSON payload)."""
        self.login()

        def _do_request():
            res = self.session.get(
                f'{BASE_URL}/report/get_data_report.php',
                params=params,
                timeout=timeout,
            )
            res.raise_for_status()
            try:
                return res.json()
            except ValueError as e:
                snippet = res.text[:200].replace('\n', ' ').strip()
                raise ValueError(
                    f'iSurvey ตอบกลับไม่ใช่ JSON '
                    f'(status={res.status_code}, '
                    f'content-type={res.headers.get("Content-Type", "?")}): '
                    f'{snippet}'
                ) from e

        try:
            return _do_request()
        except requests.exceptions.HTTPError as e:
            if e.response is None or e.response.status_code not in (401, 403):
                raise
            self._logged_in = False
            self.login()
            return _do_request()
        except ValueError:
            # JSONDecodeError — session likely expired and we got HTML back
            self._logged_in = False
            self.login()
            return _do_request()

    def fetch_all_pages(self, date_from, date_to, report_type='enquiry'):
        all_records = []
        page = 1
        start = 0

        while True:
            params = {
                'con_date': 2,
                'date_from': date_from,
                'date_to': date_to,
                'report_type': report_type,
                'page': page,
                'start': start,
                'limit': PAGE_LIMIT,
            }
            body = self.get_report_page(params, timeout=REQUEST_TIMEOUT)

            if isinstance(body, dict):
                records = body.get('arr_data', body.get('data', []))
                total = body.get('total', body.get('totalCount', 0))
            else:
                records = body
                total = len(body)

            all_records.extend(records)

            if not records or len(all_records) >= total:
                break

            page += 1
            start += PAGE_LIMIT

        return all_records, total

    def fetch_chunk(
        self, df_str, dt_str, report_type, chunk_idx,
        stop_event, event_queue,
    ):
        """Fetch all pages for a single date-range chunk.

        Puts progress events on `event_queue`, then either a `chunk_done`
        (with records) or `chunk_error` (with message) event. Aborts early
        if `stop_event` is set.
        """
        try:
            records = []
            page = 1
            start = 0

            while True:
                if stop_event.is_set():
                    return

                params = {
                    'con_date': 2,
                    'date_from': df_str,
                    'date_to': dt_str,
                    'report_type': report_type,
                    'page': page,
                    'start': start,
                    'limit': PAGE_LIMIT,
                }

                body = self.get_report_page(params, timeout=REQUEST_TIMEOUT)

                if isinstance(body, dict):
                    batch = body.get('arr_data', body.get('data', []))
                    total = body.get('total', body.get('totalCount', 0))
                else:
                    batch = body
                    total = len(body)

                records.extend(batch)

                event_queue.put(('progress', {
                    'chunk_idx': chunk_idx,
                    'chunk_fetched': len(records),
                    'chunk_total': total,
                }))

                if not batch or len(records) >= total:
                    break

                page += 1
                start += PAGE_LIMIT

            event_queue.put(('chunk_done', {
                'chunk_idx': chunk_idx,
                'records': records,
            }))
        except Exception as e:
            event_queue.put(('chunk_error', {
                'chunk_idx': chunk_idx,
                'error': str(e),
            }))


COLUMN_MAP = {
    'enquiry': [
        ('claim_no', 'เลขเคลม'),
        ('preNotifyNo', 'preNotifyNo'),
        ('notify_no', 'เลขรับแจ้ง'),
        ('survey_no', 'เลขเซอเวย์'),
        ('policy_Type', 'ประเภทเคลม'),
        ('policy_no', 'เลขกรมธรรม์'),
        ('plate_no', 'ทะเบียนรถ'),
        ('acc_detail', 'ลักษณะเหตุ'),
        ('acc_place', 'สถานที่เกิดเหตุ'),
        ('acc_amphur', 'อำเภอที่เกิดเหตุ'),
        ('acc_province', 'จังหวัดที่เกิดเหตุ'),
        ('survey_amphur', 'อำเภอที่ออกตรวจสอบ'),
        ('survey_province', 'จังหวัดที่ออกตรวจสอบ'),
        ('police_station', 'พิ้นที่สน.'),
        ('acc_verdict_desc', 'ถูก/ผิด/ร่วม/ไม่พบ/ไม่ยุติ'),
        ('empcode', 'พนักงานตรวจสอบ'),
        ('assign_reason', 'เหตุผลการจ่ายงาน'),
        ('emp_phone', 'เบอร์โทรศัพท์พนักงาน'),
        ('useOSS', 'ใช้เซอร์เวย์นอก'),
        ('branch', 'ศูนย์'),
        ('tp_insure', '(คู่กรณี) มี/ไม่มี ประกัน/ไม่มีคู่กรณี'),
        ('acc_zone', 'เขต (กท./ปม/ตจว)'),
        ('claim_Type', 'ประเภทเคลม(ว.4/นัดหมาย)'),
        ('wrkTime', 'ใน/นอก(เวลางาน)'),
        ('COArea', 'นอกพื้นที่'),
        ('service_type', 'ประเภทบริการ'),
        ('extraReq', 'ว.7'),
        ('notified_dt', 'วันที่/เวลารับแจ้ง'),
        ('dispatch_dt', 'วันที่/เวลาจ่ายงาน'),
        ('confirm_dt', 'วันที่/เวลารับงาน'),
        ('arrive_dt', 'วันที่/เวลาถึง ว.22'),
        ('cmp_arrive', 'ถึงที่เกิดเหตุ(ก่อน/หลัง คู่กรณี)'),
        ('finish_dt', 'วันที่/เวลาเสร็จงาน ว.14'),
        ('sendReport_dt', 'วันที่/เวลาส่งรายงาน'),
        ('travel_time', 'สรุปเวลา'),
        ('veh', 'การชน(รถ)'),
        ('ast', 'ทรัพย์สิน'),
        ('inj', 'ผู้บาดเจ็บ'),
        ('ctotal', 'รวม'),
        ('recover_dmg_pymt', 'จำนวนเงินเรียกร้อง'),
        ('remark', 'หมายเหตุ'),
        ('notified_name', 'ผู้รับแจ้ง'),
        ('dispatch_name', 'ผู้จ่ายงาน'),
        ('checkByName', 'ผู้ตรวจสอบงาน'),
        ('checker_dt', 'วันที่/เวลาตรวจสอบ'),
        ('stt_desc', 'สถานะงาน'),
        ('EMCSstatus', 'EMCSstatus'),
        ('EMCSby', 'EMCSby'),
        ('EMCSdate', 'EMCSdate'),
    ],
    'closeClaim': [
        ('empname', 'ผู้ปิดงาน'),
        ('close_dt', 'วันที่/เวลาตรวจสอบ'),
        ('claim_no', 'เลขเคลม'),
        ('notify_no', 'เลขรับแจ้ง'),
        ('survey_no', 'เลขเซอเวย์'),
        ('plate_no', 'ทะเบียนรถ'),
        ('acc_detail', 'ลักษณะเหตุ'),
        ('acc_place', 'สถานที่เกิดเหตุ'),
        ('notified_name', 'ผู้รับแจ้ง'),
        ('notified_dt', 'เวลารับแจ้ง'),
        ('dispatch_dt', 'เวลาจ่ายงาน'),
        ('arrive_dt', 'ถึงที่เกิดเหตุ ว.22'),
        ('finish_dt', 'เสร็จงาน ว.14'),
        ('sendReport_dt', 'ส่งรายงาน'),
        ('travel_time', 'สรุปเวลา'),
    ],
}

app = Flask(__name__)
client = ISurveyClient()


@app.route('/')
@check_basic_auth
def index():
    return render_template('index.html', column_map=COLUMN_MAP)


@app.route('/fetch', methods=['POST'])
@check_basic_auth
def fetch():
    date_from = request.form.get('date_from', '')
    date_to = request.form.get('date_to', '')
    report_type = request.form.get('report_type', 'enquiry')

    try:
        df = datetime.strptime(date_from, '%Y-%m-%d').strftime('%d/%m/%Y')
        dt = datetime.strptime(date_to, '%Y-%m-%d').strftime('%d/%m/%Y')
    except ValueError:
        return jsonify({'error': 'รูปแบบวันที่ไม่ถูกต้อง'}), 400

    try:
        records, total = client.fetch_all_pages(df, dt, report_type)
    except Exception as e:
        client._logged_in = False
        return jsonify({'error': str(e)}), 500

    columns = COLUMN_MAP.get(report_type)
    return jsonify({'total': total, 'data': records, 'columns': columns})


@app.route('/fetch-stream', methods=['POST'])
@check_basic_auth
def fetch_stream():
    date_from = request.form.get('date_from', '')
    date_to = request.form.get('date_to', '')
    report_type = request.form.get('report_type', 'enquiry')

    try:
        df_date = datetime.strptime(date_from, '%Y-%m-%d')
        dt_date = datetime.strptime(date_to, '%Y-%m-%d')
    except ValueError:
        def error_gen():
            yield f"event: error\ndata: {json.dumps({'error': 'รูปแบบวันที่ไม่ถูกต้อง'})}\n\n"
        return Response(error_gen(), mimetype='text/event-stream')

    if (dt_date - df_date).days > 730:
        def range_error():
            yield f"event: error\ndata: {json.dumps({'error': 'ช่วงวันที่เกิน 2 ปี กรุณาเลือกช่วงที่สั้นกว่านี้'})}\n\n"
        return Response(range_error(), mimetype='text/event-stream')

    # Split the date range into chunks for parallel fetching.
    chunks = []
    cursor = df_date
    idx = 0
    while cursor <= dt_date:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), dt_date)
        chunks.append((
            idx,
            cursor.strftime('%d/%m/%Y'),
            chunk_end.strftime('%d/%m/%Y'),
        ))
        cursor = chunk_end + timedelta(days=1)
        idx += 1

    def generate():
        try:
            client.login()
        except Exception as e:
            client._logged_in = False
            yield f"event: error\ndata: {json.dumps({'error': f'Login failed: {e}'})}\n\n"
            return

        deadline = time.monotonic() + 540
        stop_event = threading.Event()
        event_queue = queue.Queue()
        chunk_results = {}
        chunk_progress = {}
        chunks_total = len(chunks)
        chunks_done = 0
        error_msg = None

        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(MAX_WORKERS, chunks_total),
            thread_name_prefix='isurvey-chunk',
        )

        try:
            for chunk_idx, chunk_df, chunk_dt in chunks:
                executor.submit(
                    client.fetch_chunk,
                    chunk_df, chunk_dt, report_type,
                    chunk_idx, stop_event, event_queue,
                )

            while chunks_done < chunks_total:
                if time.monotonic() > deadline:
                    stop_event.set()
                    error_msg = 'Request timed out (เกิน 9 นาที) ลองเลือกช่วงวันที่สั้นลง'
                    break

                try:
                    ev_type, ev_data = event_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                if ev_type == 'progress':
                    chunk_progress[ev_data['chunk_idx']] = {
                        'fetched': ev_data['chunk_fetched'],
                        'total': ev_data['chunk_total'],
                    }
                    total_fetched = sum(p['fetched'] for p in chunk_progress.values())
                    total_est = sum(p['total'] for p in chunk_progress.values())
                    yield (
                        f"event: progress\ndata: "
                        f"{json.dumps({'fetched': total_fetched, 'total': total_est, 'page': chunks_done + 1})}\n\n"
                    )
                elif ev_type == 'chunk_done':
                    chunk_results[ev_data['chunk_idx']] = ev_data['records']
                    chunks_done += 1
                elif ev_type == 'chunk_error':
                    stop_event.set()
                    error_msg = ev_data['error']
                    break

            if error_msg:
                executor.shutdown(wait=False, cancel_futures=True)
                client._logged_in = False
                yield f"event: error\ndata: {json.dumps({'error': error_msg})}\n\n"
                return

            executor.shutdown(wait=True)

            all_records = []
            for i in range(chunks_total):
                all_records.extend(chunk_results.get(i, []))

            columns = COLUMN_MAP.get(report_type)
            yield (
                f"event: done\ndata: "
                f"{json.dumps({'total': len(all_records), 'data': all_records, 'columns': columns})}\n\n"
            )
        finally:
            stop_event.set()
            executor.shutdown(wait=False, cancel_futures=True)

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


if __name__ == '__main__':
    app.run(debug=True, port=int(os.getenv('PORT', 5000)))
