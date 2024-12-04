import io
import json
import os
import sys
import threading
import time
import uuid

import requests
from bs4 import BeautifulSoup
from flask import Blueprint
from flask import request
from qcloud_cos import CosConfig, CosS3Client
from requests_toolbelt import MultipartEncoder

from common.Jwt import check_token
from config.common import Config
from common.log import error
from lib.utils import report_resort, get_download_url
from service.common import get_system_variable, get_platform_user_id
from service.question import get_question_by_question_id, add_question, add_answer
from service.research import get_research_by_id, get_research, set_research_attr, get_research_brief
from service.research_attribute import get_attribute_by_research_id, update_attribute

LOG_TAG = "v2 controller research"
research_controller = Blueprint('research_controller', __name__)


@research_controller.route('/v2/research/detail')
def research_detail():
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
        file = get_research_by_id(request.args.get('research_id'))
        if len(file) == 0:
            return res
        else:
            file = file[0]
        file.pop('local_path')
        data = file
        summaries = get_attribute_by_research_id(file['uuid'], "summary")
        titles = get_attribute_by_research_id(file['uuid'], "title")
        meta = get_attribute_by_research_id(file['uuid'], 'meta_data')
        data['url'] = get_download_url(file)
        for s in summaries:
            data["research_summary"][f"i18n-{s['lang']}"] = s["value"]
        for t in titles:
            data["research_title"][f"i18n-{t['lang']}"] = t["value"]
        for h in meta:
            data['meta_data'] = h['value']
        res['data'] = data
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@research_controller.route('/v2/research/content', methods=['POST'])
def report_content():
    res = {
        'status': False,
        'data': '',
        'err_msg': ""
    }
    try:
        PATH = f"/home/ibagents/files/research/"
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        research_id = req_data.get('research_id')
        file = get_research_by_id(research_id)

        if len(file) == 0:
            res["err_msg"] = "file not found"
            return res
        else:
            file = file[0]
        if not file['source'] == 'sa':
            if not file['parse_status'] == 'parse_ok':
                res["err_msg"] = "file not parse ok"
                return res
        parsing_text = ""
        if file['source'] == 'quartr' and file['business_type'] == 'audio':
            with open(f"{PATH}{file['uuid']}.json", 'r', encoding='UTF-8') as f:
                result = json.load(f)
                merge_text = ""
                for i in result['transcript']['paragraphs']:
                    merge_text = merge_text + f"start:{i['start']}\ntext:{i['text']}\n\n "
                parsing_text = merge_text
        if file['file_type'] == 'pdf':
            config = CosConfig(Region='ap-beijing', SecretId=Config.COS_SECRET_ID, SecretKey=Config.COS_SECRET_KEY,
                               Token=None,
                               Scheme='https')
            client = CosS3Client(config)
            response = client.get_object(
                Bucket='research1-1328064767',
                Key=f"{file['uuid']}_{Config.GALPHA_PARSING_VERSION}.json"
            )
            res_json = response['Body'].read(chunk_size=sys.maxsize)
            parsing_data = json.loads(res_json)
            parsing_data = report_resort(parsing_data)
            for key in list(parsing_data.keys()):
                parsing_text = parsing_text + "\n\n" + parsing_data[key]['data']
        if file['source'] == 'fargo':
            with open(f"{PATH}{file['uuid']}.html", 'r', encoding='UTF-8') as f:
                parsing_text = f.read()
        if file['source'] == 'sa':
            parsing_text = handle_sa_content(f"{PATH}{file['uuid']}.html", research_id=file['uuid'],
                                             source_url=file['source_url'])
        res["data"] = parsing_text
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


def downloadHtml(file_url, local_file_path, header):
    try:
        
        response = requests.get(url=file_url, headers=header)
        
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(bytes(response.text, encoding='utf-8'))
            return True
    except Exception as e:
        print(str(e))


def handle_sa_content(path, research_id, source_url):
    content = None
    meta_data = get_attribute_by_research_id(research_id=research_id, attribute='meta_data')
    if meta_data:
        meta_data = meta_data[0]
        value = json.loads(meta_data['value'])
        if 'data' in value:
            content = value['data']['attributes']['content']
    headers = {
        "authority": "seekingalpha.com",
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "referer": "https://seekingalpha.com/latest-articles",
        "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Cookie": 'machine_cookie=6891739350306; _pcid=%7B%22browserId%22%3A%22m1a73cw3exuq9akm%22%7D; __pat=-14400000; xbc=%7Bkpex%7DXmodQN0PAJz1WfqmDSQ8jw; _gcl_au=1.1.1752591616.1726804969; _ga=GA1.1.1434211427.1726804969; _fbp=fb.1.1726804969788.18028926320844166; _pxvid=347c125c-7705-11ef-a563-d1d18514549c; hubspotutk=108d35cd71a3116dc9db5683231b345e; _pctx=%7Bu%7DN4IgrgzgpgThIC5QDYAMB2AnMzBGAHIqAA4xQBmAlgB6IggA0IALgJ7FR0BqAGiAL78mkWAGVmAQ2aQ6ZAOaUIzWFAAmjEBErKAkuoQA7MABtj-IA; __tae=1727436779183; __tbc=%7Bkpex%7DVE1VN54vKN1l_HQ5zK3bry9X2pt-0xP7hPmDDqFIIQQdI2V6XLHNnbTngMCegKub; _sasource=; _clck=1fygimo%7C2%7Cfqa%7C0%7C1758; __hssrc=1; pxcts=5bd7af49-91a9-11ef-a12d-a25bf54c7e5f; _igt=61daf47e-472b-4327-9ff0-c28ff72bb85d; google_one_tap=sign_in; sailthru_pageviews=1; session_id=de403757-dbde-4233-84cc-06a34fe67ed0; _uetsid=5a769f5091a911ef9957b98ffec6ef76; _uetvid=5a769fd091a911efbad53ff20e323079; sailthru_visitor=f8706bd0-60fd-4a00-a63f-b485846f9ff6; sailthru_hid=21e2d3ad134c116ad09b3d4c64f2b3b665815987edca62982c095e06e4298feaae616b43bed622fe7f7e7cfc; __hstc=234155329.108d35cd71a3116dc9db5683231b345e.1726804971248.1729734203573.1729736601525.7; __hssc=234155329.1.1729736601525; user_id=61170224; user_nick=riiri; user_devices=1%2C2; user_cookie_key=1w3nz7n; u_voc=; has_paid_subscription=true; ever_pro=1; sapu=12; user_remember_token=b01620106e573e56da7cb5d48adbdf7c3ab5091b; gk_user_access=1*premium.archived*1729736621; gk_user_access_sign=d40068045cad313a8dcf2be13761f790e75b169f; userLocalData_mone_session_lastSession=%7B%22machineCookie%22%3A%226891739350306%22%2C%22machineCookieSessionId%22%3A%226891739350306%261729734200268%22%2C%22sessionStart%22%3A1729734200268%2C%22sessionEnd%22%3A1729738439614%2C%22firstSessionPageKey%22%3A%22df1503b1-fc66-45fd-9416-8eccd64ae3d5%22%2C%22isSessionStart%22%3Afalse%2C%22lastEvent%22%3A%7B%22event_type%22%3A%22mousemove%22%2C%22timestamp%22%3A1729736639614%7D%7D; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Flatest-articles%22%2C%22pageKey%22%3A%2223f64284-052e-4076-81e5-267ce01ae81e%22%7D; _ig=61170224; sailthru_content=b6e65dd404e50fc5910df13b45c183dc2f28c8eb693d38f9b332e225c4a6e13fefa669a9d472fcd923a0a253d629f53b3c5f57e561d9315a8a9a86be8720511e5fcdd5b675fead61ac14b7518e17f855fb524416ba80b74367178ef54be32bd989e68a952abdd8585690952f1484c3b8b6f09daf178879a54eed98528b6242aa105a6c3b33449dbc0fb518a57eae2b83f0251524d6c5bdfecb6f06af6381eace; _clsk=18eb1l0%7C1729736640783%7C18%7C0%7Cl.clarity.ms%2Fcollect; _ga_KGRFF2R2C5=GS1.1.1729736590.6.1.1729736641.9.0.0; _px3=facdd81cbcc1bc288804be57703ff8fa1f7c8d7462b815037653c7b7b56df800:jQr3qSrhw/fkud/HNbMlE044lo1e5JzMeN4cO+4gsCHES0C7wGHl/UmHgtn/4QV9QVFExPJgKi/QJSogduqD6w==:1000:d37AJDoay2gMBsWKWp7OPv3m0/J4WDwZl0xIratFI4fxpn6DZfqguacwsnIoh0/Z4fY8g+G4ZpuGGCVMR2JP86zILjkzeyJbhfMafgv8Gyd15ad6rOV/We/JCXqz8wBopaStLsz/XkASlMSBikzCrJSqhrZaa7L4DPvITAWRcSK5uy/4LCWmOWvx3/g3rndzmjSxPL5Z9mHiVT28Djj69RZ/5lwnd/+Iwk8ZX8vv3kU=; _pxde=b32b241e9bfdb8c122cb838839236fd33852b8cbee2b275dc6661490b302147a:eyJ0aW1lc3RhbXAiOjE3Mjk3MzY2NDIxODIsImZfa2IiOjB9'
    }
    if not os.path.exists(path):
        downloadHtml(source_url, f'{"/home/ibagents/files/research/"}{research_id}.html', headers)
    with open(path, 'r', encoding='UTF-8') as f:
        all_content = f.read()
        responseHtml = BeautifulSoup(all_content, 'html.parser')
        summary = responseHtml.find_all(class_="mb-16 text-4x-large-b")
        if len(summary) > 0:
            summary = str(summary[0])
        summary = str(summary)

        summary2 = responseHtml.find_all(class_="mb-24 Dw1An")
        if len(summary2) > 0:
            summary2 = str(summary2)
        summary = summary + summary2


        if not content:
            content = responseHtml.find_all(
                class_="paywall-full-content hq_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                content = responseHtml.find_all(
                    class_="paywall-full-content hp_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                content = responseHtml.find_all(
                    class_="paywall-full-content hs_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                content = responseHtml.find_all(
                    class_="hq_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                content = responseHtml.find_all(
                    class_="paywall-full-content R6FbO flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            content = str(content[0])
        if len(content) < 5000:
            
            if meta_data:
                meta_value = json.loads(meta_data['value'])
                if 'id' in meta_value:
                    id = meta_value['id']
                    url = f"https://seekingalpha.com/api/v3/articles/{id}"
                    querystring = {
                        "include": "author,primaryTickers,secondaryTickers,otherTags,presentations,presentations.slides,author.authorResearch,author.userBioTags,co_authors,promotedService,sentiments"}
                    article_detail = requests.request("GET", url, headers=headers, params=querystring)
                    article_data = json.loads(article_detail.text)
                    
                    update_attribute(attribute_id=meta_data['uuid'], value=article_detail.text)
                    content = article_data['data']['attributes']['content']
        content = str(content)
        result = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Title</title></head><body>' + summary + content + "</body></html>"
    return result


@research_controller.route('/v2/research/search', methods=['POST'])
def research_search():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        page_count = req_data.get('page_count')
        page_size = req_data.get('page_size')
        search = req_data.get('search')
        params = req_data.get('params')
        if not page_size or page_size > 200:
            page_size = 200
        res['data'] = []
        research = get_research(page_count=page_count, page_size=page_size, search=search)
        for i in research:
            i['url'] = get_download_url(i)
            i.pop('local_path')
            if params:
                res['data'].append({f"{params}": i[f'{params}']})
            else:
                res['data'].append(i)
        res['status'] = True

    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res

@research_controller.route('/v2/research/test', methods=['get'])
def test():
    with open(file='/Users/leefeng/Downloads/intensity-segments.pdf', mode='rb') as fis:
        url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file/v2"
        file_content = fis
        attribute = {}
        header = {}
        file_p = {
            'filename': f"intensity-segments.pdf",
            'Content-Disposition': 'form-data;',
            'Content-Type': 'multipart/form-data',
            'file': ("/Users/leefeng/Downloads/intensity-segments.pdf", file_content, 'multipart/form-data'),
            'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"' + f',"attribute":{attribute}' + '}'
        }
        form_data = MultipartEncoder(file_p)
        header['content-type'] = form_data.content_type
        r = requests.post(url, data=form_data, headers=header, timeout=600)
        print(r)


@research_controller.route('/v2/research/parse', methods=['POST'])
def research_parse():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        file_id = req_data.get('research_id')
        is_sync = req_data.get('is_sync')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        file = get_research_by_id(file_id=file_id)

        file = file[0]
        if file['parse_status'] == "parse_ok":
            res['status'] = True
            return res
        if not file['download_status']:
            res["err_msg"] = "file not download"
            return res
        attribute = get_attribute_by_research_id(research_id=file_id, attribute='meta_data')
        if len(file) == 0:
            res["err_msg"] = "research not found"
            return json.dumps(res)
        if len(attribute) == 0:
            file['meta_data'] = None
        else:
            file['meta_data'] = attribute[0]['value']
        if is_sync:
            res['status'] = parsing_research(file)
        else:
            threading.Thread(target=parsing_research, args=(file,)).start()
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


def parsing_research(file):
    try:
        PATH = f"/home/ibagents/files/research/"
        config = CosConfig(Region='ap-beijing', SecretId=Config.COS_SECRET_ID, SecretKey=Config.COS_SECRET_KEY,
                           Token=None,
                           Scheme='https')
        client = CosS3Client(config)
        attribute = file['meta_data']
        if not attribute:
            attribute = "{}"
        url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file/v2"
        header = {}
        local_path = f"{PATH}{file['uuid']}.{file['file_type']}"
        with open(file=local_path, mode='rb') as fis:
            file_content = fis
            file_p = {
                'filename': f"{file['uuid']}.pdf",
                'Content-Disposition': 'form-data;',
                'Content-Type': 'multipart/form-data',
                'file': (local_path, file_content, 'multipart/form-data'),
                'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"' + f',"attribute":{attribute}' + '}'
            }
            form_data = MultipartEncoder(file_p)  
            header['content-type'] = form_data.content_type
            r = requests.post(url, data=form_data, headers=header, timeout=600)

            if r.status_code != 200:
                set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
                error(f"research parsing fail {r.text}")
                return False
            else:
                data = json.loads(r.text)
                if "parsed_result" not in data:
                    set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
                res = json.loads(data["parsed_result"])
                for h in res:
                    if 'attribute' in res[h]['metadata']:
                        res[h]['metadata'].pop('attribute')
                    if 'text' in res[h]['metadata']:
                        res[h]['metadata'].pop('text')
                res_json = json.dumps(res)

                
                response = client.put_object(
                    Bucket='research1-1328064767',
                    Body=res_json.encode('utf-8'),
                    Key=f"{file['uuid']}_{Config.GALPHA_PARSING_VERSION}.json",
                    EnableMD5=False
                )
                if 'ETag' in response:
                    set_research_attr(file['uuid'], 'parse_status', 'parse_ok')
                    print(f"success,{file['uuid']}")
                    return True
                    
                else:
                    error(f"research parsing {file['uuid']}")
                    set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
                    return False
    except Exception as e:
        error(message=f"research parsing {file['uuid']}", e=e)
        set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
        return False


@research_controller.route('/v2/research/embedding', methods=['POST'])
def research_embedding():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        file_id = req_data.get('research_id')
        is_sync = req_data.get('is_sync')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        file = get_research_by_id(file_id=file_id)
        if len(file) == 0:
            res["err_msg"] = "research not found"
            return json.dumps(res)
        file = file[0]
        if file['embedding_status'] == "embedding_ok":
            res['status'] = True
            return res
        if is_sync:
            res['status'] = embedding_research(file)
        else:
            threading.Thread(target=embedding_research, args=(file,)).start()
            res['status'] = True
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


def embedding_research(file):
    try:
        DIFY_TOKEN = get_system_variable("dify_token")
        if len(DIFY_TOKEN) > 0:
            DIFY_TOKEN = DIFY_TOKEN[0]["value"]
        config = CosConfig(Region='ap-beijing', SecretId=Config.COS_SECRET_ID, SecretKey=Config.COS_SECRET_KEY,
                           Token=None,
                           Scheme='https')

        client = CosS3Client(config)
        set_research_attr(file['uuid'], 'embedding_status', 'embedding')
        res = search_dify_file(file['uuid'], file_type='pdf')
        if res['total'] > 0:
            dify_file_id = res['data'][0]['id']
            if res['data'][0]['indexing_status'] == "completed":
                set_research_attr(file['uuid'], 'embedding_status', 'embedding_ok')
            return True
        else:
            PATH = f"/home/ibagents/files/research/"
            url = "https://ops.fargoinsight.com/console/api/files/upload?source=datasets"
            querystring = {"source": "datasets"}
            path = f"{PATH}{file['uuid']}.pdf"
            with open(file=path, mode='rb') as fis:
                headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Authorization": f"Bearer {DIFY_TOKEN}",
                    "Connection": "keep-alive",
                    "Content-Type": "multipart/form-data; boundary=---011000010111000001101001",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-site",
                    "content-type": "multipart/form-data"
                }
                file_content = fis
                file_p = {
                    'file': (file['uuid'] + ".pdf", file_content, 'application/pdf'),
                }
                form_data = MultipartEncoder(file_p)  
                headers['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=headers, params=querystring, timeout=500)
                data = json.loads(r.text)
                if not data['id']:
                    set_research_attr(file['uuid'], 'embedding_status', 'embedding_fail')
                    error(f"sync_embedding upload pdf file fail {r.text}")
                    return
                else:
                    dify_file_id = data['id']
        response = client.get_object(
            Bucket='research1-1328064767',
            Key=f"{file['uuid']}_{Config.GALPHA_PARSING_VERSION}.json"
        )
        res_json = response['Body'].read(chunk_size=sys.maxsize)
        if res_json:
            timestamp = int(file['publish_time'].timestamp())
            embedding_res = dify_embedding(dify_file_id, file['uuid'], json.loads(res_json), DIFY_TOKEN, timestamp,
                                           file['source'])
            if embedding_res:
                set_research_attr(file['uuid'], 'embedding_status', 'embedding_ok')
                return True
            else:
                set_research_attr(file['uuid'], 'embedding_status', 'embedding_fail')
                return False
        else:
            set_research_attr(file['uuid'], 'embedding_status', 'embedding_fail')
            return False
    except Exception as e:
        error(message=f"research parsing {file['uuid']}", e=e)
        set_research_attr(file['uuid'], 'embedding_status', 'embedding_fail')
        return False


def search_dify_file(file_id, file_type):
    DATASET_ID = "d0b959ce-4a9f-4df8-8347-50adbd68a010"
    result = False
    
    url = f"http://ops.fargoinsight.com/v1/datasets/{DATASET_ID}/documents?keyword={file_id}.{file_type}"
    headers = {"Authorization": "Bearer dataset-Xdljx2aImEN2uvSZE4oAzLZq"}
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        error(f"dify search file fail{response.text}")
    else:
        result = json.loads(response.text)
    return result


def dify_embedding(upload_id: str, file_id: str, parsing_data: dict, dify_token: str, publish_time: int, source: str):
    result = False
    DIFY_REPOSITORY = "d0b959ce-4a9f-4df8-8347-50adbd68a010"
    try:
        parsing_data = report_resort(parsing_data)
        upload_data = {}
        
        for key in list(parsing_data.keys()):
            parsing_data[key]['metadata'].pop('attribute', None)
            parsing_data[key]['metadata'].pop('text', None)
            upload_data[key] = parsing_data[key]
            
        datas = {
            "file_id": file_id,
            "publish_time": publish_time,
            "source": source,
            "parsing_data": upload_data
        }
        url = f'https://ops.fargoinsight.com/console/api/datasets/{DIFY_REPOSITORY}/documents/custom'

        payload = {
            "data_source": {
                "type": "upload_file",
                "info_list": {
                    "data_source_type": "upload_file",
                    "file_info_list": {"file_ids": [upload_id]}
                }
            },
            "indexing_technique": "high_quality",
            "process_rule": {
                "rules": {},
                "mode": "automatic"
            },
            "doc_form": "text_model",
            "doc_language": "Chinese",
            "retrieval_model": {
                "search_method": "hybrid_search",
                "reranking_enable": True,
                "reranking_model": {
                    "reranking_provider_name": "jina",
                    "reranking_model_name": "jina-reranker-v1-base-en"
                },
                "top_k": 7,
                "score_threshold_enabled": False,
                "score_threshold": 0.5
            },
            "custom": datas
        }
        headers = {
            "authority": "ops.fargoinsight.com",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "content-type": "application/json",
            "origin": "https://ops.fargoinsight.com",
            "referer": "https://ops.fargoinsight.com/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents/create",
            "Authorization": f"Bearer {dify_token}"}

        response = requests.request("POST", url, json=payload, headers=headers)
        if response.status_code != 200:
            error(message=f"dify embedding api error:{response.text}")
            return False
        for i in range(0, 60):
            
            res = search_dify_file(file_id=file_id, file_type='pdf')
            if res['total'] == 0:
                continue
            if res['data'][0]['indexing_status'] == "completed":
                return True
            time.sleep(5)
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
        error(e=e, message=f"dify embedding api fail:{str(e)}")
    return result


@research_controller.route('/v2/research/ask', methods=['POST'])
def ask_research():
    app_key = "app-RbA2guFSgaQNC0CuMEtX3X90"
    res = {
        'status': False,
        'data': {},
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        question_id = req_data.get('question_id')
        report_id = req_data.get('report_id')
        question_body = req_data.get('question_body')
        user_id = req_data.get('user_id')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        
        user_id = get_platform_user_id(user_id, token_dict[1]['platform'])

        if question_id:
            
            question = get_question_by_question_id(question_id)
            
            if len(question) == 0:
                res['err_msg'] = "questionId not exist"
                return res
            else:
                return res
        question_id = uuid.uuid1()
        result = add_question(question_id=question_id, user_id=user_id, reports_id=report_id, question=question_body,
                              source=token_dict[1]['platform'])
        if result:
            
            url = "http://ops.fargoinsight.com/v1/completion-messages"
            payload = {
                "inputs": {
                    "docid": report_id,
                    "question": question_body
                },
                "response_mode": "blocking",
                "user": "system"
            }
            headers = {
                "Authorization": f"Bearer {app_key}",
                "Content-Type": "application/json",
                "content-type": "application/json"
            }
            response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
            if response.status_code != 200:
                error(f"call api fail {response.text}")
            answer = json.loads(response.text)['answer']
            res['data'] = answer
            add_res = add_answer(question_id=question_id, answer=answer, lang="zh-CN", source=f"dify:{app_key}")
            if add_res:
                pass
            res["status"] = True
            return res
        else:
            res["err_msg"] = "save question fail"
            return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)


@research_controller.route('/v2/research/brief', methods=['POST'])
def research_brief():
    res = {
        "status": False,
        "data": [],
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        files_id = req_data.get('research_id')
        params = req_data.get('params')
        file = get_research_brief(files_id=files_id)
        if len(file) == 0:
            return res
        for i in file:
            temp = {}
            temp['uuid'] = i['uuid']
            for h in params:
                temp[h] = i[h]
            res['data'].append(temp)
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        error(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
