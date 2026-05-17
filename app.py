import concurrent.futures
import logging
import os
import queue
import secrets
import threading
import time
from datetime import datetime, timedelta
from functools import wraps

import orjson
import requests
from dotenv import load_dotenv
from flask import (
    Flask, Response, jsonify, redirect, render_template, request,
    session, url_for,
)
from flask_compress import Compress
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

# Configure root logging before anything else creates a logger. Without this
# all our app.logger / module-level log.* calls would be swallowed by Flask's
# default "no handler" setup under Gunicorn — making prod debugging painful.
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
)
log = logging.getLogger('se-report')


def _jdumps(obj):
    """JSON-encode via orjson (5-10x faster than stdlib on big payloads).

    Returns str so existing f-string concatenations in the SSE generators
    keep working. The big win is the final done event (full dataset, can
    be 30-50 MB JSON for heavy users) — orjson cuts that serialization
    time from ~2s to ~300ms on a typical fetch.
    """
    return orjson.dumps(obj).decode()


# Bumped when releasing user-visible changes; displayed in the login footer
# so admins can confirm which build is live without checking the server.
APP_VERSION = '1.5.0'

# Tuning constants for parallel chunk fetching.
# iSurvey rate-limits concurrent connections; 8 workers failed in prior tests.
CHUNK_DAYS = 30
PAGE_LIMIT = 5000
MAX_WORKERS = 4
REQUEST_TIMEOUT = 120

# Per-user override: usernames in this set are "heavy" iSurvey accounts
# (statistics analysts who occasionally fetch a full year of data). They get
# a larger per-request chunk pool; everyone else stays on MAX_WORKERS, which
# is plenty for the dominant day-by-day / 3-4-month fetch pattern. Matched
# case-insensitively against the iSurvey login name.
FAST_MODE_USERS = {'noppadol', 'noppadols'}
FAST_MAX_WORKERS = 6


def _max_workers_for(username):
    """Per-user chunk-pool size, honoring the FAST_MODE_USERS whitelist."""
    if username and username.lower() in FAST_MODE_USERS:
        return FAST_MAX_WORKERS
    return MAX_WORKERS


# Defensive process-wide cap on concurrent iSurvey HTTP calls, pooled across
# every user's chunk threads. Per-account quotas (~8 from prior testing) are
# already protected by per-user max_workers (4 default, 6 for FAST_MODE_USERS);
# this global cap guards the IP-level total when many users fetch at once.
# Sized so up to 5 fast-mode users can run at full FAST_MAX_WORKERS=6 in
# parallel without queueing — regular users barely register because they use
# 1-4 slots transiently. Raise this if monitoring shows threads frequently
# parked at the semaphore.
ISURVEY_MAX_CONCURRENT = 30
_ISURVEY_SEMAPHORE = threading.Semaphore(ISURVEY_MAX_CONCURRENT)

BASE_URL = 'https://cloud.isurvey.mobi/web/php'


class ISurveyClient:
    def __init__(self, username, password):
        # Each user logs in with their own iSurvey credentials; the client
        # keeps them in instance state so the auto re-login path can replay
        # them when iSurvey's HTTP session expires mid-fetch. Credentials
        # never leave the server process (in particular, they are NOT
        # written to the Flask session cookie — the cookie only holds an
        # opaque sid that maps to this instance via _USER_CLIENTS).
        self.username = username
        self.password = password
        self.session = requests.Session()
        # 2 retries + 0.5s backoff caps worst-case page fetch at roughly
        # 3 attempts * REQUEST_TIMEOUT + 1.5s. The previous 3 retries / 1s
        # backoff could spend ~6 min on a single bad page, leaving very
        # little room before the 9-min generator deadline; tighter budget
        # surfaces upstream issues faster instead of hiding them in retry.
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        # Pool sized for FAST_MAX_WORKERS so heavy users running 6 chunk
        # threads in parallel don't trip urllib3's "connection pool is
        # full" warning (which forces TCP teardown/reconnect per request).
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=FAST_MAX_WORKERS * 2,
            pool_maxsize=FAST_MAX_WORKERS * 2,
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
            if not self.username or not self.password:
                raise RuntimeError(
                    'iSurvey credentials ไม่ครบ — กรุณา login ใหม่'
                )
            # Block on the global iSurvey semaphore so a burst of fresh
            # logins doesn't blow past the upstream rate limit. Released
            # before the response body is parsed below since parsing is
            # CPU-bound and the network slot is no longer needed.
            with _ISURVEY_SEMAPHORE:
                res = self.session.post(
                    f'{BASE_URL}/login.php',
                    data={'username': self.username, 'password': self.password},
                    timeout=15,
                )
                res.raise_for_status()
            # iSurvey returns 200 even on bad credentials, with one of two
            # shapes depending on the failure mode:
            #   1. JSON {"success": false, "message": "..."} — bad creds.
            #   2. An HTML page containing the login form — session-expired
            #      or other server-side errors.
            # Treat anything that isn't an explicit JSON success or a non-
            # login HTML page as a failure.
            payload = None
            try:
                payload = res.json()
            except ValueError:
                payload = None
            if isinstance(payload, dict) and payload.get('success') is False:
                raise RuntimeError(
                    'iSurvey login ล้มเหลว — ' +
                    (payload.get('message') or 'ตรวจสอบ username / password')
                )
            if payload is None:
                body_lower = res.text.lower()
                if '<form' in body_lower and 'password' in body_lower:
                    raise RuntimeError(
                        'iSurvey login ล้มเหลว — ตรวจสอบ username / password'
                    )
            self._logged_in = True

    def get_report_page(self, params, timeout=60):
        """Fetch a single report page. Auto re-login once on 401/403 or invalid
        JSON (which usually means the session expired and iSurvey returned an
        HTML login page instead of the expected JSON payload)."""
        self.login()

        def _do_request():
            # Hold the global semaphore only for the network round-trip;
            # JSON parsing happens on the already-buffered body so it can
            # safely run without holding an iSurvey slot.
            with _ISURVEY_SEMAPHORE:
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

# gzip everything text-y, INCLUDING text/event-stream. Flask-Compress
# wraps streaming responses incrementally (Z_SYNC_FLUSH per chunk) so
# SSE events still arrive in real time — but the giant final done event
# (full dataset) gets gzip'd, cutting the wire payload 5-10x. Login/
# index HTML and JSON error responses ride along for free.
app.config['COMPRESS_MIMETYPES'] = [
    'text/html', 'text/css', 'text/xml', 'text/plain',
    'application/json', 'application/javascript',
    'text/event-stream',
]
# Default 500-byte threshold is fine; small SSE progress events skip
# compression (overhead would dwarf the saving).
Compress(app)

# SECRET_KEY signs the Flask session cookie. In production a missing key
# would silently fall back to a per-process random one — fine for local
# dev (users just re-login on restart) but catastrophic under multi-worker
# Gunicorn where each worker would sign with a different key. Raise loudly
# when FLASK_ENV=production so misconfig surfaces at boot, not when a
# user's session inexplicably stops working.
SECRET_KEY = os.getenv('SECRET_KEY')
if not SECRET_KEY:
    if os.getenv('FLASK_ENV', '').lower() == 'production':
        raise RuntimeError(
            "SECRET_KEY env var is required when FLASK_ENV=production. "
            "Generate one with: "
            "python -c \"import secrets; print(secrets.token_urlsafe(32))\""
        )
    SECRET_KEY = secrets.token_urlsafe(32)
    log.warning(
        "SECRET_KEY not set — using ephemeral key. All sessions will be "
        "invalidated on restart. Set SECRET_KEY in .env for stability."
    )
app.secret_key = SECRET_KEY

# Cookie security defaults. SECURE=True requires HTTPS; for local dev over
# plain HTTP set SESSION_COOKIE_SECURE=false in .env or the login cookie
# won't be sent back by the browser. SameSite=Lax + HttpOnly together
# block the basic CSRF/XSS cookie-exfiltration paths.
_cookie_secure_env = os.getenv('SESSION_COOKIE_SECURE', 'true').lower()
app.config.update(
    SESSION_COOKIE_SECURE=_cookie_secure_env not in ('false', '0', 'no', ''),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
)
# Idle/auto-expire window for the Flask session cookie. The session is
# marked permanent on login and refreshed (slid) on each request through
# the require_login decorator, so this is effectively an inactivity timer.
app.permanent_session_lifetime = timedelta(hours=8)


@app.context_processor
def _inject_template_globals():
    """Expose APP_VERSION + csrf_token to every Jinja template.

    csrf_token is empty on the login page (no session yet) but populated
    once login completes — login.html doesn't need it because /login POST
    is the bootstrap path that mints the token in the first place.
    """
    return {
        'app_version': APP_VERSION,
        'csrf_token': session.get('csrf_token', ''),
    }


# ---------------------------------------------------------------------------
# Per-user authentication
#
# We don't run a user database. iSurvey itself acts as the identity
# provider: the login form takes a username/password, we try to log them
# in to iSurvey, and on success we stash the resulting ISurveyClient
# instance keyed by a random sid. The browser only ever sees that sid (in
# the signed Flask session cookie); the actual credentials live in
# _USER_CLIENTS in this process's memory and disappear on restart.
#
# In-memory by design: a Gunicorn multi-worker deploy would need a shared
# store (e.g. Flask-Session with filesystem backend) so a login handled
# by worker A is visible to worker B. For the current single-process dev
# / docker setup that's not a concern.
# ---------------------------------------------------------------------------
_USER_CLIENTS = {}                  # sid -> ISurveyClient
_USER_LAST_USED = {}                # sid -> monotonic timestamp of last touch
_USER_CLIENTS_LOCK = threading.Lock()

# Reap entries whose Flask session cookie would already have expired.
# Without this, a user who logs in / closes the tab / never logs out
# leaves their ISurveyClient pinned in memory until process restart.
SESSION_IDLE_SECONDS = int(app.permanent_session_lifetime.total_seconds())
_SWEEP_INTERVAL_SECONDS = 600       # 10-min throttle; sweep is O(N) over a small N
_last_sweep_ts = 0.0                # protected by _USER_CLIENTS_LOCK


def _maybe_sweep_stale(now):
    """Drop sids untouched for longer than the session lifetime.

    Call with _USER_CLIENTS_LOCK held. Throttled so the per-request hot
    path doesn't walk the dict on every call. A swept entry would have
    failed Flask's own cookie-expiry check anyway, so this only reclaims
    memory — it never logs anyone out who would still have been valid.
    """
    global _last_sweep_ts
    if now - _last_sweep_ts < _SWEEP_INTERVAL_SECONDS:
        return
    _last_sweep_ts = now
    cutoff = now - SESSION_IDLE_SECONDS
    stale = [sid for sid, ts in _USER_LAST_USED.items() if ts < cutoff]
    for sid in stale:
        _USER_CLIENTS.pop(sid, None)
        _USER_LAST_USED.pop(sid, None)
    if stale:
        log.info(
            "Swept %d stale session(s); %d active remain",
            len(stale), len(_USER_CLIENTS),
        )


def get_user_client():
    """Return the ISurveyClient bound to the current Flask session, or None."""
    sid = session.get('sid')
    if not sid:
        return None
    now = time.monotonic()
    with _USER_CLIENTS_LOCK:
        _maybe_sweep_stale(now)
        client = _USER_CLIENTS.get(sid)
        if client is not None:
            _USER_LAST_USED[sid] = now
    return client


def _register_user_client(client):
    """Generate a fresh sid, store the client, and wire it to the session."""
    sid = secrets.token_urlsafe(32)
    now = time.monotonic()
    with _USER_CLIENTS_LOCK:
        _USER_CLIENTS[sid] = client
        _USER_LAST_USED[sid] = now
    session.permanent = True
    session['sid'] = sid
    session['username'] = client.username
    # Mint a fresh CSRF token per session. Embedded in the index template
    # as a <meta> tag and validated on every state-changing POST via
    # require_csrf — protects /logout and /fetch-stream from cross-origin
    # form submits that would otherwise inherit the user's session cookie.
    session['csrf_token'] = secrets.token_urlsafe(32)
    return sid


def _drop_current_user_client():
    """Pop the session's sid and any registered ISurveyClient."""
    sid = session.pop('sid', None)
    session.pop('username', None)
    session.pop('csrf_token', None)
    if sid:
        with _USER_CLIENTS_LOCK:
            _USER_CLIENTS.pop(sid, None)
            _USER_LAST_USED.pop(sid, None)


def _csrf_ok():
    """Constant-time compare of submitted CSRF token to the session's."""
    expected = session.get('csrf_token')
    if not expected:
        return False
    provided = (
        request.headers.get('X-CSRF-Token')
        or request.form.get('csrf_token')
        or ''
    )
    return secrets.compare_digest(expected, provided)


def _sse_error(message):
    """Build a one-shot SSE Response carrying a single error event.

    The trailing ': end' comment is a flush trampoline — some reverse
    proxies (Traefik in Dokploy, nginx) hold the last write until the
    connection sees another byte, so without it the client may never
    receive the error.
    """
    def gen():
        yield f"event: error\ndata: {_jdumps({'error': message})}\n\n"
        yield ': end\n\n'
    return Response(gen(), mimetype='text/event-stream')


def require_login(f):
    """Gate a route behind a logged-in iSurvey session.

    HTML routes redirect to /login when unauthenticated. /fetch-stream
    returns an SSE-shaped error instead so the frontend's event-stream
    reader can surface it without choking on an HTML redirect body.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if get_user_client() is None:
            if request.path == '/fetch-stream':
                return _sse_error('Session หมดอายุ — กรุณา login ใหม่')
            return redirect(url_for('login_page'))
        # Touch the session to slide the expiry window forward.
        session.permanent = True
        session.modified = True
        return f(*args, **kwargs)
    return decorated


def require_csrf(f):
    """Validate the per-session CSRF token. Place after @require_login.

    For /fetch-stream we return an SSE-shaped error so the in-page
    progress UI can surface it cleanly; for plain POST handlers we
    return 403 JSON. Frontend embeds the token in a <meta> tag and
    sends it back as X-CSRF-Token (fetch API) or a hidden form field
    (logout button).
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _csrf_ok():
            log.warning(
                "CSRF rejected for %s from %s (ua=%s)",
                request.path, request.remote_addr,
                request.headers.get('User-Agent', '?')[:80],
            )
            if request.path == '/fetch-stream':
                return _sse_error(
                    'CSRF token ไม่ถูกต้อง — กรุณา refresh หน้านี้แล้ว login ใหม่'
                )
            return jsonify({'error': 'CSRF token invalid'}), 403
        return f(*args, **kwargs)
    return decorated


@app.route('/login', methods=['GET'])
def login_page():
    if get_user_client() is not None:
        return redirect(url_for('index'))
    return render_template('login.html', error=None, username='')


@app.route('/login', methods=['POST'])
def login_submit():
    username = (request.form.get('username') or '').strip()
    password = request.form.get('password') or ''
    if not username or not password:
        return render_template(
            'login.html', error='กรุณากรอก username และ password',
            username=username,
        ), 400
    client = ISurveyClient(username, password)
    try:
        client.login()
    except Exception as e:
        log.warning("Login rejected for user %r: %s", username, e)
        return render_template(
            'login.html', error=str(e), username=username,
        ), 401
    # Replace any previous session for this browser before issuing the new sid.
    _drop_current_user_client()
    _register_user_client(client)
    log.info("Login OK for user %r", username)
    return redirect(url_for('index'))


@app.route('/logout', methods=['POST'])
@require_csrf
def logout():
    _drop_current_user_client()
    return redirect(url_for('login_page'))


@app.route('/')
@require_login
def index():
    username = session.get('username', '')
    return render_template(
        'index.html',
        column_map=COLUMN_MAP,
        username=username,
        # Drives the lightning-bolt icon in the user-chip — visual cue
        # that this account is in the FAST_MODE_USERS whitelist and gets
        # FAST_MAX_WORKERS instead of MAX_WORKERS.
        is_fast_user=username.lower() in FAST_MODE_USERS,
    )


@app.route('/fetch-stream', methods=['POST'])
@require_login
@require_csrf
def fetch_stream():
    date_from = request.form.get('date_from', '')
    date_to = request.form.get('date_to', '')
    report_type = request.form.get('report_type', 'enquiry')

    try:
        df_date = datetime.strptime(date_from, '%Y-%m-%d')
        dt_date = datetime.strptime(date_to, '%Y-%m-%d')
    except ValueError:
        return _sse_error('รูปแบบวันที่ไม่ถูกต้อง')

    if df_date > dt_date:
        return _sse_error('วันที่เริ่มต้นต้องไม่อยู่หลังวันที่สิ้นสุด')

    if (dt_date - df_date).days > 730:
        return _sse_error('ช่วงวันที่เกิน 2 ปี กรุณาเลือกช่วงที่สั้นกว่านี้')

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

    # Capture the user's ISurveyClient at request time. The streaming
    # generator runs after the route returns, so we need a stable
    # reference rather than re-reading the Flask session inside it.
    client = get_user_client()
    user_max_workers = _max_workers_for(client.username)

    def generate():
        try:
            client.login()
        except Exception as e:
            log.exception("fetch_stream: pre-fetch login failed for %r", client.username)
            yield f"event: error\ndata: {_jdumps({'error': f'Login failed: {e}'})}\n\n"
            yield ': end\n\n'
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
            max_workers=min(user_max_workers, chunks_total),
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
                    # Extrapolate per-chunk avg to ALL chunks so total_est
                    # stays roughly stable as new chunks come online. The
                    # naive sum-of-reported behaviour caused the progress
                    # percentage to bounce backward whenever a fresh chunk
                    # reported its first page (denominator suddenly grew
                    # while numerator stayed put).
                    avg = (
                        sum(p['total'] for p in chunk_progress.values())
                        / len(chunk_progress)
                    )
                    total_est = int(avg * chunks_total)
                    yield (
                        f"event: progress\ndata: "
                        f"{_jdumps({'fetched': total_fetched, 'total': total_est, 'chunks_done': chunks_done, 'chunks_total': chunks_total})}\n\n"
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
                log.warning(
                    "fetch_stream: user=%r range=%s..%s aborted: %s",
                    client.username, date_from, date_to, error_msg,
                )
                yield f"event: error\ndata: {_jdumps({'error': error_msg})}\n\n"
                # SSE comment to force a flush — some reverse proxies
                # (Traefik in Dokploy, nginx) hold the last write until
                # the connection sees another byte. Without this the
                # terminal event can stall in the proxy buffer and the
                # browser never sees it.
                yield ': end\n\n'
                return

            executor.shutdown(wait=True)

            all_records = []
            for i in range(chunks_total):
                all_records.extend(chunk_results.get(i, []))

            columns = COLUMN_MAP.get(report_type)
            yield (
                f"event: done\ndata: "
                f"{_jdumps({'total': len(all_records), 'data': all_records, 'columns': columns})}\n\n"
            )
            # Same flush trampoline as the error branch above. The 'done'
            # payload is large (full dataset), so the proxy is more likely
            # to buffer it; this trailing comment guarantees the prior
            # event reaches the client before the generator exits.
            yield ': end\n\n'
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
