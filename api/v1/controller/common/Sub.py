import datetime
import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.ReportService import add_error_log
from api.v1.service.UserSubService import get_all_sub_user

LOG_TAG = "sys sub api"
sub_controller = Blueprint('sub_controller', __name__)


@sub_controller.route('/v1/symbol/sub/user', methods=['POST'])
def symbol_sub_user():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        symbol = req_data.get('symbol')
        platform = req_data.get('platform')
        add_res = get_all_sub_user(symbol=symbol, platform=platform)
        if add_res:
            res['data'] = add_res
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
