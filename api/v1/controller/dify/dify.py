import datetime
import json
import re

import requests
from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.ReportService import add_error_log

LOG_TAG = "sys dify api"
dify_controller = Blueprint('dify_controller', __name__)


@dify_controller.route('/v1/dify/chat/create', methods=['POST'])
def chat_create():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        url = "https://ops.fargoinsight.com/api/passport"
        headers = {
            "authority": "ops.fargoinsight.com",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "authorization": "Bearer undefined",
            "content-type": "application/json",
            "referer": "https://ops.fargoinsight.com/chat/AaD6l1I3RShkzOMR",
            "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            "sec-ch-ua-mobile": '?0',
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "x-app-code": f"{req_data['app_code']}"
        }

        response = requests.request("GET", url, headers=headers, timeout=(120.0, 300.0))
        if response.status_code != 200:
            res['err_msg'] = "get access token fail"
            return json.dumps(res)
        access_token = json.loads(response.text)['access_token']
        headers["Authorization"] = f"Bearer {access_token}"
        url = "https://ops.fargoinsight.com/api/site"
        response = requests.request("GET", url, headers=headers, timeout=(120.0, 300.0))
        if response.status_code != 200:
            res['err_msg'] = "get app_id fail"
            return json.dumps(res)
        app_id = json.loads(response.text)['app_id']
        url = "https://ops.fargoinsight.com/api/chat-messages"
        payload = {
            "inputs": req_data['inputs'],
            "query": "
            "response_mode": "streaming",
            "conversation_id": "",
        }

        response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 500.0))
        if response.status_code != 200:
            res['err_msg'] = "request chat-messages fail"
            return json.dumps(res)


        stream_data_lines = response.text.strip().split('\n\n')

        json_pattern = re.compile(r'data:\s*({.*})')

        first_conversation_id = None
        for line in stream_data_lines:
            match = json_pattern.match(line)
            if match:
                json_data = match.group(1)

                parsed_data = json.loads(json_data)

                first_conversation_id = parsed_data.get('conversation_id')
                if first_conversation_id:
                    break

        res['status'] = True
        res['data'] = {
            "token": req_data['app_code'],
            "token_value": access_token,
            "conversationIdInfo": app_id,
            "conversationIdInfo_value": first_conversation_id
        }
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)


@dify_controller.route('/v1/dify/chat/message', methods=['POST'])
def chat_message():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        url = "https://ops.fargoinsight.com/api/chat-messages"
        payload = {
            "inputs": req_data['inputs'],
            "query": req_data['query'],
            "conversation_id": f"{req_data['conversation_id']}",
            "response_mode": "streaming"
        }
        headers = {
            "Authorization": f"Bearer {req_data['access_token']}",
            "Content-Type": "application/json",
            "content-type": "application/json"
        }

        response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 500.0))
        if response.status_code != 200:
            res['err_msg'] = "request chat-messages fail"
            return json.dumps(res)
        res['status'] = True
        res['data'] = response.text
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)
