import datetime
import json

import requests

from config.common import DEFAULTS
from lib.mysql import query_dict, execute


def get_system_variable(key):
    sql = f"SELECT * FROM TB_Config WHERE name=%s limit 1"
    data = query_dict(sql, (key,))
    return data


def get_platform_user_id(p_user_id, platform):
    sql = f"select * from TB_API_User where p_user_id=%s and platform=%s"
    data = query_dict(sql, (p_user_id, platform))
    if data[0]:
        return data[0]['p_user_id']
    else:
        return ""


def get_pando_wechat_access_code():
    key_name = "pando_wechat_access_key"
    key = get_system_variable(key=key_name)
    if key:
        temp_time = datetime.datetime.now() - datetime.timedelta(hours=1.0)
        if int(key[0]['updated_time'].timestamp()) > int(temp_time.timestamp()):
            return key[0]['value']
        else:
            
            url = f"https://api.weixin.qq.com/cgi-bin/token?" \
                  f"grant_type=client_credential&appid={DEFAULTS['PANDO_APPID']}&secret={DEFAULTS['PANDO_SECRET']}"
            response = requests.request("GET", url, timeout=(120.0, 300.0))
            if not response.status_code == 200:
                return None
            access_token = json.loads(response.text)['access_token']
            sql = f"update TB_Config set value=%s,updated_time=%s where name=%s"
            execute(sql, (access_token, datetime.datetime.now(), key_name))
            return access_token
