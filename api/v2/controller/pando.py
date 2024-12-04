import base64
import datetime
import json
import uuid

from flask import Blueprint, Response, stream_with_context, make_response, send_file
from flask import request

from common.Jwt import check_token
from common.log import error
import requests

from lib.utils import truncate_string
from service.common import get_system_variable, get_pando_wechat_access_code
from service.conversation import add_conversation, add_user_conversation_share, \
    get_conversation_by_user_id, get_conversations_by_last_id, delete_conversations_by_id, \
    update_conversation_name_by_id, add_share_conversation, get_conversation_by_id
from service.conversation_share import add_share, get_conversation_id_from_share
from service.message import add_message, get_messages, get_messages_by_ids, add_share_messages, add_share_user_messages
from service.user import get_platform_by_id

LOG_TAG = "v2 controller pando"
pando_controller = Blueprint('pando_controller', __name__)
tool_cn_name_dict = {
    "ResearchLibRetrieval30": "检索多篇研报",
    "MarketInfoRetrieval": "检索指定上市公司信息",
    "jina_reader": "检索互联网信息",
    "SingleReportRetrieval30": "检索研报",
    "searxng_search": "检索互联网信息",
    "ProductSearch": "检索指定产品信息",
}

@pando_controller.route('/MaBCdlRNfe.txt')
def wechat_verify():
    file_path = f'controller/MaBCdlRNfe.txt'
    response = make_response(send_file(file_path, as_attachment=False))
    response.headers["Content-Disposition"] = f"inline;filename=MaBCdlRNfe.txt"
    return response

@pando_controller.route('/nref4MOj1v.txt')
def wechat_verify2():
    file_path = f'controller/nref4MOj1v.txt'
    response = make_response(send_file(file_path, as_attachment=False))
    response.headers["Content-Disposition"] = f"inline;filename=nref4MOj1v.txt"
    return response



@pando_controller.route('/v2/chat-messages', methods=['POST'])
def chat_messages():
    data = {}
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        params = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        conversation_id = params.get('conversation_id', None)
        share_id = params.get('share_id', None)
        query = params.get('query')
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        else:
            token_dict = token_dict[1]
        if conversation_id or not share_id:
            return Response(stream_with_context(generate(
                conversation_id=conversation_id,
                user=token_dict['user_id'],
                query=query,
                token_dict=token_dict)), status=200, mimetype='text/event-stream')
        if share_id:

            share_conversation = get_conversation_id_from_share(share_id=share_id)
            if share_conversation:
                share_conversation = share_conversation[0]
            else:
                res['err_msg'] = "share conversation not found"
                return res
            conversation = get_conversation_by_id(conversation_id=share_conversation['share_conversation_id'])
            if conversation:
                conversation = conversation[0]
            else:
                res['err_msg'] = 'conversation not found'

            messages = get_messages(conversation_id=conversation['uuid'], limit=50)
            messages.reverse()
            merge_text = "The following is a record of the previous conversation:\n\n"
            for message in messages:
                merge_text = merge_text + f"query:{message['query']}\n\nanswer:{message['answer']}\n\n"
            merge_text = merge_text + f"\n\nThis is my question:{query}"
            return Response(stream_with_context(generate_share(
                conversation_id="",
                conversation_name=conversation['name'],
                share_conversation_id=share_conversation['share_conversation_id'],
                user=token_dict['user_id'],
                query=merge_text,
                single_query=query,
                token_dict=token_dict)), status=200, mimetype='text/event-stream')
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/messages', methods=['GET'])
def messages():
    data = {}
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        conversation_id = request.args.get('conversation_id')
        first_id = request.args.get('first_id')
        limit = int(request.args.get('limit', 20))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        else:
            token_dict = token_dict[1]

        if get_conversation_by_user_id(user_id=token_dict['user_id'], conversation_id=conversation_id):
            messages = get_messages(conversation_id=conversation_id, first_id=first_id, limit=limit)
            messages.reverse()
            res['data'] = messages
            res['status'] = True
        else:
            res['status'] = False
            res['err_msg'] = 'conversation not found'
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/share/messages', methods=['POST'])
def share_messages():
    data = {}
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        req_data = request.get_json()
        platform_id = req_data.get('platform_id')
        platform_key = req_data.get('platform_key')
        platform = get_platform_by_id(platform_id)
        if platform:
            platform = platform[0]
        else:
            res['err_msg'] = "platform not found"
            return res
        if not platform_key == platform['key']:
            res['err_msg'] = "platform key error"
            return res
        share_id = req_data.get('share_id')
        first_id = request.args.get('first_id')
        limit = int(request.args.get('limit', 20))

        share_conversation = get_conversation_id_from_share(share_id=share_id)
        if share_conversation:
            messages = get_messages(conversation_id=share_conversation[0]['share_conversation_id'], first_id=first_id,
                                    limit=limit)
            messages.reverse()
            res['data'] = messages
            res['status'] = True
        else:
            res['status'] = False
            res['err_msg'] = 'conversation not found'
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/audio-to-text', methods=['POST'])
def audio_to_text():
    data = {}
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        file = request.files['file']
        url = "http://ops.fargoinsight.com/v1/audio-to-text"
        headers = {
            "Authorization": "Bearer app-qJUNAjP85SDLcqKPAz84yBH3",
        }
        files = {
            'file': (file.filename, file.stream, file.content_type),
        }
        response = requests.post(url, files=files, headers=headers)
        if response.status_code == 200:
            res['data'] = json.loads(response.text)['text']
            res['status'] = True
        else:
            res['status'] = False
            res['err_msg'] = f'audio to text fail {response.text}'
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/conversation/share', methods=['POST'])
def create_share_conversation():
    data = {}
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        params = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        else:
            token_dict = token_dict[1]
        share_id = params.get('share_id')
        share_conversation = get_conversation_id_from_share(share_id=share_id)
        if share_conversation:
            share_conversation = share_conversation[0]
        else:
            res['err_msg'] = "share conversation not found"
            return res
        conversation = get_conversation_by_id(conversation_id=share_conversation['share_conversation_id'])
        if conversation:
            conversation = conversation[0]
        else:
            res['err_msg'] = 'conversation not found'

        messages = get_messages(conversation_id=conversation['uuid'], limit=50)
        messages.reverse()
        merge_text = "The following is a record of the previous conversation:\n\n"
        for message in messages:
            merge_text = merge_text + f"query:{message['query']}\n\nanswer:{message['answer']}\n\n"
        new_conversation_id = create_dify_conversation(conversation_id='', user=token_dict['user_id'], query=merge_text)
        if add_user_conversation_share(conversation_id=new_conversation_id,
                                       name=conversation['name'],
                                       user_id=token_dict['user_id'],
                                       now_time=datetime.datetime.now()):
            if add_share_user_messages(share_conversation_id=conversation['uuid'],
                                       new_conversation_id=new_conversation_id):
                res['status'] = True
                res['data'] = {"conversation_id": new_conversation_id}
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/conversation/share/create', methods=['POST'])
def create_share_id():
    res = {
        "status": False,
        "data": None,
        "err_msg": ""
    }
    try:
        params = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        conversation_id = params.get('conversation_id')
        message_id_list = params.get('message_id_list')
        if len(message_id_list) == 0:
            res['err_msg'] = 'message_id_list is null'
            return res
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        else:
            token_dict = token_dict[1]

        conversation = get_conversation_by_id(conversation_id=conversation_id)
        if conversation:
            conversation = conversation[0]
        else:
            res['err_msg'] = 'conversation not found'
            return res
        messages = get_messages_by_ids(message_id_list=message_id_list, conversation_id=conversation_id)
        if len(messages) == 0:
            res['err_msg'] = 'messages not found'
            return res
        now_time = datetime.datetime.now()
        share_conversation_id = f"{uuid.uuid1()}"
        share_id = f"{uuid.uuid1()}".replace('-', '')
        if add_share_conversation(conversation_id=share_conversation_id, name=conversation['name'], now_time=now_time):
            add_share_messages(messages=messages, share_conversation_id=share_conversation_id)
            if add_share(share_id=share_id,
                         share_conversation_id=share_conversation_id,
                         share_user_id=token_dict['user_id'],
                         now_time=now_time):
                res['data'] = {"share_id": share_id}
                res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@pando_controller.route('/v2/conversations', methods=['GET'])
def conversations():
    data = {}
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        last_id = request.args.get('last_id')
        limit = request.args.get('limit', 20)
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        else:
            token_dict = token_dict[1]

        data = get_conversations_by_last_id(user_id=token_dict['user_id'], last_id=last_id, limit=limit)
        now = datetime.datetime.now()
        data_res = []
        today_midnight = datetime.datetime.combine(now.date(), datetime.time.min)
        start_of_week = now - datetime.timedelta(days=now.weekday())
        start_of_week_midnight = datetime.datetime.combine(start_of_week.date(), datetime.time.min)
        start_of_month = now.replace(day=1)
        start_of_month_midnight = datetime.datetime.combine(start_of_month.date(), datetime.time.min)
        start_of_year = now.replace(month=1, day=1)
        start_of_year_midnight = datetime.datetime.combine(start_of_year.date(), datetime.time.min)
        for i in data:
            if i['created_time'] >= today_midnight:
                i['created_time'] = int(i['created_time'].timestamp())
                i['time_type'] = 'day'
                data_res.append(i)
            elif i['created_time'] >= start_of_week_midnight:
                i['created_time'] = int(i['created_time'].timestamp())
                i['time_type'] = 'week'
                data_res.append(i)
            elif i['created_time'] >= start_of_month_midnight:
                i['created_time'] = int(i['created_time'].timestamp())
                i['time_type'] = 'month'
                data_res.append(i)
            elif i['created_time'] >= start_of_year_midnight:
                i['created_time'] = int(i['created_time'].timestamp())
                i['time_type'] = 'year'
                data_res.append(i)
            else:
                i['created_time'] = int(i['created_time'])
                i['time_type'] = 'year'
                data_res.append(i)

        res['data'] = data_res
        res['status'] = True
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/conversation', methods=['DELETE'])
def delete_conversation():
    res = {
        "status": False,
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        conversation_id = request.args.get('conversation_id')
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        else:
            token_dict = token_dict[1]







        if delete_conversations_by_id(user_id=token_dict['user_id'], conversation_id=conversation_id):
            res['status'] = True
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/conversation/name', methods=['POST'])
def rename_conversation():
    res = {
        "status": False,
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        params = request.get_json()
        name = params.get('name')
        conversation_id = params.get('conversation_id')
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        else:
            token_dict = token_dict[1]










        if update_conversation_name_by_id(user_id=token_dict['user_id'], conversation_id=conversation_id, name=name):
            res['status'] = True
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


@pando_controller.route('/v2/config', methods=['POST'])
def config():
    data = {}
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        req_data = request.get_json()
        platform_id = req_data.get('platform_id')
        platform_key = req_data.get('platform_key')
        platform = get_platform_by_id(platform_id)
        if platform:
            platform = platform[0]
        else:
            res['err_msg'] = "platform not found"
            return res
        if not platform_key == platform['key']:
            res['err_msg'] = "platform key error"
            return res
        if platform['user'] == 'pando':
            platform_config = get_system_variable("pando_config")
            res['status'] = True
            res['data'] = json.loads(platform_config[0]['value'])
            return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
        return res


def generate(conversation_id, user, query, token_dict):
    try:
        url = "https://ops.fargoinsight.com/v1/chat-messages"
        payload = {
            "inputs": {},
            "query": query,
            "response_mode": "streaming",
            "conversation_id": conversation_id,
            "user": user,
            "files": []
        }
        headers = {
            "Authorization": "Bearer app-qJUNAjP85SDLcqKPAz84yBH3",
            "Content-Type": "application/json",
            "content-type": "application/json"
        }

        response = requests.request("POST", url, json=payload, headers=headers, stream=True)
        if response.status_code != 200:
            yield "data: Error: Unable to fetch stream data\n\n"
            return

        first_line = True
        for line in response.iter_lines():
            if line:

                try:
                    line_str = line.decode('utf-8')
                    if line_str.startswith("data: "):
                        line_str = line_str[len("data: "):]
                    data = json.loads(line_str)
                    if data.get('tool') and data.get('tool') in tool_cn_name_dict:
                        data['tool'] = tool_cn_name_dict[data.get('tool')]
                    if data.get('tool') and 'dataset' in data.get('tool'):
                        data['tool'] = '知识库'
                    if 'answer' in data and data.get('answer') == '':
                        continue
                    else:
                        if first_line and 'answer' in data and data.get('answer').startswith("\n\n"):
                            data['answer'] = data['answer'].lstrip("\n")
                        first_line = False
                    yield f"{'data: ' + json.dumps(data)}\n\n\n"
                    if not data.get('event') == 'agent_thought':
                        continue
                    thought = data.get('thought', '')
                    if not thought:
                        continue


                    res_conversation_id = data.get('conversation_id')
                    res_message_id = data.get('message_id')
                    datetime_now = datetime.datetime.now()

                    if conversation_id == res_conversation_id:
                        result = add_message(uuid=res_message_id, query=query, answer=thought,
                                             conversation_id=res_conversation_id, time=datetime_now)
                        print(result)
                    else:

                        name = truncate_string(s=query, max_length=32)
                        result = add_conversation(uuid=res_conversation_id,
                                                  name=name,
                                                  user_id=token_dict['user_id'],
                                                  time=datetime_now,
                                                  message_id=res_message_id,
                                                  query=query, answer=thought)
                        print(result)

                except Exception as e:
                    print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')

    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


def generate_share(conversation_id, user, query, token_dict, conversation_name, share_conversation_id, single_query):
    try:
        url = "https://ops.fargoinsight.com/v1/chat-messages"
        payload = {
            "inputs": {},
            "query": query,
            "response_mode": "streaming",
            "conversation_id": conversation_id,
            "user": user,
            "files": []
        }
        headers = {
            "Authorization": "Bearer app-qJUNAjP85SDLcqKPAz84yBH3",
            "Content-Type": "application/json",
            "content-type": "application/json"
        }

        response = requests.request("POST", url, json=payload, headers=headers, stream=True)
        if response.status_code != 200:
            yield "data: Error: Unable to fetch stream data\n\n"
            return

        first_line = True
        for line in response.iter_lines():
            if line:

                try:
                    line_str = line.decode('utf-8')
                    if line_str.startswith("data: "):
                        line_str = line_str[len("data: "):]
                    data = json.loads(line_str)
                    if data.get('tool') and data.get('tool') in tool_cn_name_dict:
                        data['tool'] = tool_cn_name_dict[data.get('tool')]
                    if data.get('tool') and 'dataset' in data.get('tool'):
                        data['tool'] = '知识库'
                    if 'answer' in data and data.get('answer') == '':
                        continue
                    else:
                        if first_line and 'answer' in data and data.get('answer').startswith("\n\n"):
                            data['answer'] = data['answer'].lstrip("\n")
                        first_line = False
                    yield f"{'data: ' + json.dumps(data)}\n\n\n"
                    if not data.get('event') == 'agent_thought':
                        continue
                    thought = data.get('thought', '')
                    if not thought:
                        continue


                    res_conversation_id = data.get('conversation_id')
                    res_message_id = data.get('message_id')
                    datetime_now = datetime.datetime.now()

                    if add_user_conversation_share(conversation_id=res_conversation_id,
                                                   name=conversation_name,
                                                   user_id=token_dict['user_id'],
                                                   now_time=datetime.datetime.now()):
                        add_share_user_messages(share_conversation_id=share_conversation_id,
                                                new_conversation_id=res_conversation_id)
                        add_message(uuid=res_message_id, query=single_query, answer=thought,
                                    conversation_id=res_conversation_id, time=datetime_now)
                except Exception as e:
                    print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


def create_dify_conversation(conversation_id, user, query):
    try:
        url = "https://ops.fargoinsight.com/v1/chat-messages"
        payload = {
            "inputs": {},
            "query": query,
            "response_mode": "streaming",
            "conversation_id": conversation_id,
            "user": user,
            "files": []
        }
        headers = {
            "Authorization": "Bearer app-qJUNAjP85SDLcqKPAz84yBH3",
            "Content-Type": "application/json",
            "content-type": "application/json"
        }

        response = requests.request("POST", url, json=payload, headers=headers, stream=True)
        if response.status_code != 200:
            return None

        for line in response.iter_lines():
            if line:
                try:
                    line_str = line.decode('utf-8')
                    if line_str.startswith("data: "):
                        line_str = line_str[len("data: "):]
                    data = json.loads(line_str)
                    if not data.get('event') == 'agent_thought':
                        continue
                    thought = data.get('thought', '')
                    if not thought:
                        continue
                    return data.get('conversation_id')
                except Exception as e:
                    print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


@pando_controller.route('/v2/conversation/qrcode', methods=['POST'])
def get_conversation_qrcode():
    res = {
        "status": False
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        access_token = get_pando_wechat_access_code()
        if not access_token:
            res['err_msg'] = "access_token get fail"
            return res










        url = f"https://api.weixin.qq.com/wxa/getwxacodeunlimit?access_token={access_token}"
        payload = req_data
        headers = {
            "Content-Type": "application/json",
            "content-type": "application/json"
        }
        response = requests.request("POST", url, json=payload, headers=headers, timeout=(300.0, 600.0))
        if response.status_code != 200:
            res['err_msg'] = f"req qrcode fail {response.text}"
            return res
        base64_encoded = base64.b64encode(response.content)

        base64_string = base64_encoded.decode('utf-8')
        res['data'] = base64_string
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
