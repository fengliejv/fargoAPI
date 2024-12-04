import json
import uuid

import requests
from flask import Blueprint
from flask import request

from common.Jwt import create_token, check_token
from common.log import error
from config.common import DEFAULTS
from lib.WXBizDataCrypt import WXBizDataCrypt
from service.common import get_pando_wechat_access_code
from service.user import get_platform_by_id, get_user_by_id, get_user_by_platform_phone, add_user

LOG_TAG = "v2 controller user"
user_controller = Blueprint('user_controller', __name__)


@user_controller.route('/v2/login', methods=['POST'])
def get_token():
    res = {
        "status": False
    }
    try:
        req_data = request.get_json()
        platform_id = req_data.get('platform_id')
        platform_key = req_data.get('platform_key')
        user_id = req_data.get('user_id')
        user_key = req_data.get('user_pass')
        platform = get_platform_by_id(platform_id)
        user = get_user_by_id(user_id)
        if platform:
            platform = platform[0]
        else:
            res['err_msg'] = "platform not found"
            return
        if user:
            user = user[0]
        else:
            res['err_msg'] = "user not found"
            return
        if not platform_key == platform['key']:
            res['err_msg'] = "platform key error"
            return res
        if not user_key == user['pass']:
            res['err_msg'] = "user key error"
            return res
        token_dict = dict()
        token_dict['platform_id'] = platform_id
        token_dict['user_id'] = user_id
        res['token'] = create_token(**token_dict)
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@user_controller.route('/v2/login/wechat', methods=['POST'])
def get_wechat_token():
    res = {
        "status": False
    }
    try:
        req_data = request.get_json()
        code = req_data.get('code')
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
        access_token = get_pando_wechat_access_code()
        if not access_token:
            res['err_msg'] = "access_token get fail"
            return res
        url = f"https://api.weixin.qq.com/wxa/business/getuserphonenumber?access_token={access_token}"
        payload = {
            "code": code
        }
        headers = {
            "Content-Type": "application/json",
            "content-type": "application/json"
        }
        response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
        if response.status_code != 200:
            res['err_msg'] = "req getuserphonenumber fail"
            return res
        phone_res = json.loads(response.text)
        if not 'phone_info' in phone_res:
            res['err_msg'] = f"{response.text}"
            return res
        phone = phone_res['phone_info']['phoneNumber']
        
        user = get_user_by_platform_phone(platform_id=platform_id, phone=phone)
        token_dict = dict()
        token_dict['platform_id'] = platform
        if user:
            token_dict['user_id'] = user[0]['uuid']
        else:
            user_id = f"{uuid.uuid1()}"
            if not add_user(user_id=user_id, platform_id=platform_id, phone=phone):
                res['err_msg'] = "register fail"
                return res
            token_dict['user_id'] = user_id
        encryptedData = req_data.get('encryptedData')
        if encryptedData:
            jsCode = req_data.get('js_code')
            url=f"https://api.weixin.qq.com/sns/jscode2session?appid={DEFAULTS['PANDO_APPID']}&secret={DEFAULTS['PANDO_SECRET']}&js_code={jsCode}&grant_type=authorization_code"
            response = requests.request("GET", url, headers=headers, timeout=(120.0, 300.0))
            if response.status_code != 200:
                res['err_msg'] = "req jscode2session fail"
                return res
            appId = DEFAULTS['PANDO_APPID']
            sessionKey = json.loads(response.text)["session_key"]
            iv = req_data.get('iv')
            pc = WXBizDataCrypt(appId, sessionKey)
            res['data'] = pc.decrypt(encryptedData, iv)
        res['token'] = create_token(**token_dict)
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@user_controller.route('/v2/token', methods=['GET'])
def refresh_token():
    res = {
        "status": False
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if token_dict[0]:
            res['token'] = create_token(**token_dict[1])
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
