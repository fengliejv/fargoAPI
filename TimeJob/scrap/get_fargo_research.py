import datetime
import json
import os
import time
import re
from pip._vendor import requests

from service.ReportService import *
from service.ResearchService import add_research, add_research_attribute
from service.SystemService import get_system_variable, update_system_variable
from lib.Common.utils import clean_none

PATH = f"/home/ibagents/files/research/"
CLIENT_ID = 'cioinsight-backend'
CLIENT_SECRET = '0258d90f-fa98-4b16-916c-51c8a38c3a46'
ARTICLE_TABLE = 'native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad'
FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'
API_TOKEN = ''


def downloadHtml(file_url, local_file_path, header):
    time.sleep(1)
    try:

        response = requests.get(url=file_url, headers=header)

        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(bytes(response.text, encoding='utf-8'))
            add_info_log(message=f"网页下载成功{file_url}")
            print(f'网页下载成功 {file_url}')
            return True
        else:
            add_error_log(message=f"网页下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        print("下载网页时发生错误:", str(e))
        add_fatal_log(message=f"下载网页时发生错误:{str(e)}")


def get_api_token():

    url = "https://auth.easyview.com.hk/realms/Easyview-News-Platform-Realm/protocol/openid-connect/token"
    payload = f"client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&grant_type=client_credentials"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "content-type": "application/x-www-form-urlencoded"
    }
    res = requests.request("POST", url, data=payload, headers=headers)
    if res.status_code != 200:
        return 'fail'
    return json.loads(res.text)['access_token']


def get_fargo_research():
    try:
        fargo_np_newest_id = int(get_system_variable("fargo_np_newest_id")[0]['value'])
        API_TOKEN = get_api_token()

        url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/articles?page=1&pageSize=1&sortBy=articleId&sortOrder=desc&thenOrder=desc'
        headers = {"Authorization": f'Bearer {API_TOKEN}'}
        res = requests.request("GET", url, headers=headers)
        remote_newest_id = json.loads(res.text)['articles'][0]['id']
        print(remote_newest_id)
        while (fargo_np_newest_id < remote_newest_id):
            try:
                url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/article/{fargo_np_newest_id}'
                headers = {"Authorization": f'Bearer {API_TOKEN}'}
                article_res = requests.request("GET", url, headers=headers)
                if article_res.status_code != 200:
                    fargo_np_newest_id += 1
                    continue
                d = json.loads(article_res.text)
                lang = list(d['titles'].keys())[0]
                if "zh_CN" in list(d['titles'].keys()):
                    lang = "zh_CN"
                p_key = f"{uuid.uuid1()}"
                create_time = datetime.datetime.now()
                file_path = f"{PATH}{p_key}.html"
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(d['contents'][lang])
                download_status = os.path.isfile(file_path)
                fargo_np_newest_id += 1
                if not update_system_variable(name="fargo_np_newest_id", value=f"{fargo_np_newest_id}"):
                    continue
                if not add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                              value=json.dumps(clean_none(d['metadata'])),
                                              create_time=create_time):
                    continue
                add_research(p_key=p_key,
                             publish_time=datetime.datetime.strptime(d['metadata']['audit']["publishTime"],
                                                                     '%Y-%m-%dT%H:%M:%SZ'),
                             parse_status='parse_ok',
                             business_type='',
                             source='fargo', title=d['titles'][lang],
                             download_status=download_status, create_time=create_time,
                             file_type='html')

            except Exception as e:
                error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
                print(error)
                add_fatal_log(e=e)
    except Exception as e:
        error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        print(error)
        add_fatal_log(e=e)


def get_fargo_research_time(start_id=31001, end_id=33050):
    try:
        fargo_np_newest_id = start_id
        API_TOKEN = get_api_token()

        url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/articles?page=1&pageSize=1&sortBy=articleId&sortOrder=desc&thenOrder=desc'
        headers = {"Authorization": f'Bearer {API_TOKEN}'}
        res = requests.request("GET", url, headers=headers)
        remote_newest_id = end_id
        print(remote_newest_id)
        while (fargo_np_newest_id < remote_newest_id):
            try:
                url = f'https://news-platform.easyview.com.hk/api/v1/channel/fargoApp/article/{fargo_np_newest_id}'
                headers = {"Authorization": f'Bearer {API_TOKEN}'}
                article_res = requests.request("GET", url, headers=headers)
                if article_res.status_code != 200:
                    fargo_np_newest_id += 1
                    print(f'获取报告失败{article_res.text}')
                    continue
                d = json.loads(article_res.text)
                lang = list(d['titles'].keys())[0]
                if "zh_CN" in list(d['titles'].keys()):
                    lang = "zh_CN"
                p_key = f"{uuid.uuid1()}"
                create_time = datetime.datetime.now()
                file_path = f"{PATH}{p_key}.html"
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(d['contents'][lang])
                download_status = os.path.isfile(file_path)
                fargo_np_newest_id += 1
                if not update_system_variable(name="fargo_np_newest_id", value=f"{fargo_np_newest_id}"):
                    continue
                if not add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                              value=json.dumps(clean_none(d['metadata'])),
                                              create_time=create_time):
                    continue
                add_research(p_key=p_key,
                             publish_time=datetime.datetime.strptime(d['metadata']['audit']["publishTime"],
                                                                     '%Y-%m-%dT%H:%M:%SZ'),
                             parse_status='parse_ok',
                             business_type='',
                             source='fargo', title=d['titles'][lang],
                             download_status=download_status, create_time=create_time,
                             file_type='html')

            except Exception as e:
                error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
                print(error)
                add_fatal_log(e=e)
    except Exception as e:
        error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        print(error)
        add_fatal_log(e=e)


if __name__ == '__main__':
    get_fargo_research()
