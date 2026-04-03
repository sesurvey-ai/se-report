import os
from datetime import datetime
from functools import wraps

import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request

load_dotenv()


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
        self._logged_in = False

    def login(self):
        if self._logged_in:
            return
        res = self.session.post(
            f'{BASE_URL}/login.php',
            data={
                'username': os.getenv('ISURVEY_USER'),
                'password': os.getenv('ISURVEY_PASS'),
            },
            timeout=15,
        )
        res.raise_for_status()
        self._logged_in = True

    def fetch_all_pages(self, date_from, date_to, report_type='enquiry'):
        self.login()
        all_records = []
        page = 1
        start = 0
        limit = 200

        while True:
            res = self.session.get(
                f'{BASE_URL}/report/get_data_report.php',
                params={
                    'con_date': 2,
                    'date_from': date_from,
                    'date_to': date_to,
                    'report_type': report_type,
                    'page': page,
                    'start': start,
                    'limit': limit,
                },
                timeout=30,
            )
            res.raise_for_status()
            body = res.json()

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
            start += limit

        return all_records, total


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


if __name__ == '__main__':
    app.run(debug=True, port=5000)
