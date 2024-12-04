import datetime
import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.ReportService import add_error_log
from api.v1.service.UserService import get_user

LOG_TAG = "sys user api"
user_controller = Blueprint('user_controller', __name__)


@user_controller.route('/v1/user/search', methods=['POST'])
def search_user():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        key_words = req_data.get('key_words')
        platform = req_data.get('platform')
        data = None
        if platform == 'wechat':
            data = get_user(key_words=key_words)
        if len(data) > 0:
            res['data'] = data[0]
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
