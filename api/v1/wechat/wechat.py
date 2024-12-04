import datetime
import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.LtvBankService import add_ltv_bank
from api.v1.service.ReportService import add_error_log

LOG_TAG = "sys wechat api"
wechat_controller = Blueprint('wechat_controller', __name__)


@wechat_controller.route('/v1/test', methods=['POST'])
def ltv_bank_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        nodes = req_data.get('nodes')
        bank_name = req_data.get('bank_name')
        add_res = add_ltv_bank(bank_code=bank_code, nodes=nodes, bank_name=bank_name)
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
