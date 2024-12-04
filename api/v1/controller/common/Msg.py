import datetime
import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.MsgQueueService import add_msg_queue
from api.v1.service.ReportService import add_error_log

LOG_TAG = "sys msg api"
msg_controller = Blueprint('msg_controller', __name__)


@msg_controller.route('/v1/message/add', methods=['POST'])
def message_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        msg_list = req_data.get('msg_list')
        count = 0
        for i in msg_list:
            try:
                receive_user = i['receive_user']
                type = i['type']
                msg = i['msg']
                action = i['action']
                send_user = i['send_user']
                msg_type = i['msg_type']
                send_time = i['send_time']
                if not send_time:
                    send_time = datetime.datetime.now()
                add_res = add_msg_queue(type=type, receiver_user=receive_user, msg=msg, action=action,
                                        send_user=send_user, send_time=send_time, msg_type=msg_type)
                count = count + add_res
            except Exception as e:
                print(str(e))
        res['data'] = count
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
