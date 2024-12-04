import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token, create_token
from api.v1.service.APIUserService import add_user, get_user, get_platform
from api.v1.service.ReportService import add_error_log
from api.v1.service.SystemService import set_system_variable

LOG_TAG = "sys common api"
common_controller = Blueprint('common_controller', __name__)


@common_controller.route('/v1/identify/token')
def get_token():
    status = False
    err_msg = ""
    token = ""
    try:

        user = get_platform(request.args.get('platform'))
        if user:
            user = user[0]
        else:
            err_msg = "user unauthorized"
        if request.args.get('key') == user['key']:
            token = create_token(user["user"])
            status = True
        else:
            err_msg = "key error"
    except Exception as e:
        err_msg = str(e)
        add_error_log(message=err_msg, source=LOG_TAG, e=e)
    return json.dumps({
        'status': status,
        'data': {'token': token},
        'err_msg': err_msg
    })


@common_controller.route('/v1/system/variable/update')
def system_variable_update():
    status = False
    err_msg = ""
    try:

        token = request.args.get('token')
        if not token == "c72bd8b63e857ec0b839b4f12f76c51c":
            err_msg = "user unauthorized"
        else:
            if set_system_variable(request.args.get('key'), request.args.get('value')):
                status = True
    except Exception as e:
        err_msg = str(e)
        add_error_log(message=err_msg, source=LOG_TAG, e=e)
    return json.dumps({
        'status': status,
        'err_msg': err_msg
    })


@common_controller.route('/v1/identify/add')
def user_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        if get_user(token_dict[1]["platform"], request.args.get('user_id')):
            res["status"] = True
            res["err_msg"] = "user already exist"
            return json.dumps(res)
        result = add_user(token_dict[1]["platform"], request.args.get('user_id'))
        if result:
            res["status"] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)
