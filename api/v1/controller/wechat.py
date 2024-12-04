import base64
import datetime
import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.LtvBankService import add_ltv_bank
from api.v1.service.ReportService import add_error_log, get_titles, get_parsed_summary
from api.v1.service.WechatBotService import get_not_send_report, get_send_report_source, get_newest_report_by_symbol, \
    get_all_wechat_user, update_wechat_user, add_wechat_user, get_all_not_send_report, set_send_history
from api.v1.service.WechatService import get_wechat_user, get_all_symbol, get_search_symbol, set_cancel_symbol, \
    add_new_sub
from service.WechatService import get_search_sub

LOG_TAG = "sys wechat api"
wechat_controller = Blueprint('wechat_controller', __name__)


@wechat_controller.route('/v1/test')
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


@wechat_controller.route('/v1/wechat/command', methods=['POST'])
def wechat_command():
    res = {
        'status': False,
        'err_msg': "",
        'data': {}
    }
    try:
        data = None
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        command = req_data['command']
        key_word = None
        
        name = str(base64.b64encode(req_data['NickName'].encode("utf-8")))
        sign = str(base64.b64encode(req_data['Signature'].encode("utf-8")))
        location = req_data['Province'] + req_data['City']
        user = None
        if not command == "list symbol":
            user = get_wechat_user(name=name, sign=sign, location=location)
            if len(user) == 0:
                res['err_msg'] = "user not found"
                return res
            user = user[0]
        if "key_word" in req_data:
            key_word = req_data['key_word']
        if command == "list symbol":
            data = get_all_symbol()
        elif command == "cancel subs":
            cancel_ok = []
            for i in key_word:
                symbol = get_search_symbol(i)
                if len(symbol) == 0:
                    res['err_msg'] = "symbol not found"
                    return res
                count = set_cancel_symbol(user['uuid'], symbol[0]['symbol'])
                if count:
                    cancel_ok.append(symbol[0]['symbol'])
            if cancel_ok:
                res['status'] = True
                res['data'] = f"取消订阅{json.dumps(cancel_ok)}成功！"
                return res
            else:
                res['err_msg'] = "cancel fail"
                return res
        elif command == "cancel sub":
            symbol = get_search_symbol(key_word)
            if len(symbol) == 0:
                res['err_msg'] = "symbol not found"
                return res
            count = set_cancel_symbol(user['uuid'], symbol[0]['symbol'])
            if count:
                res['status'] = True
                res['data'] = f"取消订阅{symbol[0]['symbol']}成功！"
                return res
            else:
                res['err_msg'] = "cancel fail"
                return res
        elif command == "show sub":
            data = get_search_sub(user['uuid'])
            if len(data) == 0:
                res['status'] = False
                res['err_msg'] = "您暂未订阅任何标的"
                return res
        elif command == "add sub":
            symbol = get_search_symbol(key_word)
            if len(symbol) == 0:
                res['err_msg'] = "add symbol not found"
                return res
            count = add_new_sub(user['uuid'], symbol[0]['symbol'])
            if count > 0:
                res['status'] = True
                res['data'] = f"订阅{symbol[0]['symbol']}成功!"
                return res
        elif command == "add subs":
            sub_ok = []
            for i in key_word:
                symbol = get_search_symbol(i)
                if len(symbol) == 0:
                    res['err_msg'] = "add symbol not found"
                    return res
                count = add_new_sub(user['uuid'], symbol[0]['symbol'])
                if count > 0:
                    sub_ok.append(symbol[0]['symbol'])
            res['status'] = True
            res['data'] = f"订阅{json.dumps(sub_ok)}成功!"
            return res
        elif command == "check update":
            reports = get_not_send_report(user['uuid'])
            data = []
            for i in reports:
                temp = {}
                temp['NickName'] = i['nick_name']
                temp['Signature'] = i['sign']
                temp['Location'] = i['location']
                
                param1 = get_send_report_source(i['file_id'])
                if len(param1) > 0:
                    param1 = param1[0]
                    temp['source'] = param1['source']
                    titles = get_titles(i['file_id'])
                    temp['title'] = ""
                    if len(titles) > 0:
                        temp['title'] = titles[0]['title']
                    temp['title_en'] = param1['title']
                    temp['publish_time'] = param1['publish_time']
                    temp['symbol'] = param1['symbol']
                    temp['url'] = f"https://files.fargoinsight.com/file/{i['file_id']}.pdf"
                summaries = get_parsed_summary(i['file_id'])
                temp['summary'] = ""
                req_lang = "zh-CN"
                for s in summaries:
                    if s["lang"] == req_lang:
                        temp['summary'] = s["summary"]
                data.append(temp)
            if len(data) == 0:
                res['status'] = False
                res['err_msg'] = "暂无更新"
                return res
        elif command == "get newest":
            symbol = get_search_symbol(key_word)
            if len(symbol) == 0:
                res['err_msg'] = "symbol not found"
                return res
            temp = {}
            param1 = get_newest_report_by_symbol(symbol[0]['symbol'])
            if len(param1) > 0:
                param1 = param1[0]
                temp['source'] = param1['source']
                titles = get_titles(param1['uuid'])
                temp['title'] = ""
                if len(titles) > 0:
                    temp['title'] = titles[0]['title']
                temp['title_en'] = param1['title']
                temp['publish_time'] = param1['publish_time']
                temp['symbol'] = param1['symbol']
                temp['url'] = f"https://files.fargoinsight.com/file/{param1['uuid']}.pdf"
                summaries = get_parsed_summary(param1['uuid'])
                temp['summary'] = ""
                req_lang = "zh-CN"
                for s in summaries:
                    if s["lang"] == req_lang:
                        temp['summary'] = s["summary"]
                res['status'] = True
                res['data'] = temp
                return res
        if len(data) > 0:
            res['status'] = True
            res['data'] = data
            return res
        else:
            res['status'] = True
            res['data'] = {}
            return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)


@wechat_controller.route('/v1/wechat/report', methods=['POST'])
def user_update():
    res = {
        'status': False,
        'data': [],
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        users = get_all_wechat_user()
        for i in req_data:
            name = str(base64.b64encode(i['NickName'].encode("utf-8")))
            sign = str(base64.b64encode(i['Signature'].encode("utf-8")))
            location = i['Province'] + i['City']
            exist = False
            for h in users:
                if h['nick_name'] == name and h['sign'] == sign and (not h['location'] == location):
                    update_wechat_user(user_id=h['uuid'], name=name, location=location, sign=sign)
                    exist = True
                elif h['nick_name'] == name and (not h['sign'] == sign) and h['location'] == location:
                    update_wechat_user(user_id=h['uuid'], name=name, location=location, sign=sign)
                    exist = True
                elif (not h['nick_name'] == name) and h['sign'] == sign and h['location'] == location:
                    update_wechat_user(user_id=h['uuid'], name=name, location=location, sign=sign)
                    exist = True
            if not exist:
                add_wechat_user(name=name, location=location, sign=sign)
        res['status'] = True

        reports = get_all_not_send_report()
        data = []
        history_param = []
        for i in reports:
            temp = {}
            temp['NickName'] = i['nick_name']
            temp['Signature'] = i['sign']
            temp['Location'] = i['location']
            
            param1 = get_send_report_source(i['file_id'])
            if len(param1) > 0:
                param1 = param1[0]
                temp['source'] = param1['source']
                temp['title_en'] = param1['title']
                titles = get_titles(i['file_id'])
                temp['title'] = ""
                if len(titles) > 0:
                    temp['title'] = titles[0]['title']
                temp['publish_time'] = param1['publish_time']
                temp['symbol'] = param1['symbol']
                temp['url'] = f"https://files.fargoinsight.com/file/{i['file_id']}.pdf"
                history_param.append((datetime.datetime.now(), i['id']))
            summaries = get_parsed_summary(i['file_id'])
            temp['summary'] = ""
            req_lang = "zh-CN"
            for s in summaries:
                if s["lang"] == req_lang:
                    temp['summary'] = s["summary"]
            data.append(temp)
        if set_send_history(history_param) > 0:
            res['data'] = data
            res['status'] = True

    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
