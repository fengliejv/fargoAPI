import json

import requests
from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.LtvBankService import add_ltv_bank, update_ltv_bank, get_ltv_bank_by_code, get_ltv_bank_all, \
    del_ltv_bank, add_bank_ltv
from api.v1.service.ReportService import add_error_log

LOG_TAG = "sys exchange api"
exchange_ltv_controller = Blueprint('exchange_ltv_controller', __name__)


@exchange_ltv_controller.route('/v1/exchange/rate/get')
def exchange_rate_get():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        url = f"https://api.apilayer.com/exchangerates_data/latest?symbols={req_data['']}&base=CNY"

        payload = {}
        headers = {
            "apikey": "yJmlySeDi17SEAWjp0MH4fD9eesmV8kP"
        }
        response = requests.request("GET", url, headers=headers, data=payload)

        status_code = response.status_code
        result = response.text
        if status_code == 200:
            res['status'] = True
            res['data'] = result
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
