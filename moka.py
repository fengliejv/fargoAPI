import datetime
import json
import sys
import time

import html2text
import requests
from qcloud_cos import CosConfig, CosS3Client
from requests_toolbelt import MultipartEncoder

from api.common.app import query_dict
from lib.Common.utils import report_resort
from service.ReportService import add_error_log
from service.SystemService import get_system_variable

GALPHA_PARSING_VERSION = "1.1"
DIFY_REPOSITORY = "2ac7b955-8c16-4b1c-ab28-c8bfbd5adcc2"
DATASET_ID = "2ac7b955-8c16-4b1c-ab28-c8bfbd5adcc2"
SecretId = 'AKIDLk5E8nDMGx1rTr21obJp7B3tdQLAOcjb'
SecretKey = 'zkG87QficM1VjhT2OZVFpAFJEzZyOckn'
PATH = f"/home/ibagents/files/research/"
IB_SOURCE = ['ubs', 'ms', 'gs', 'cicc', 'jp']


def search_dify_file(file_id, file_type):
    result = False
    
    url = f"http://ops.fargoinsight.com/v1/datasets/{DATASET_ID}/documents?keyword={file_id}.{file_type}"
    headers = {"Authorization": "Bearer dataset-Xdljx2aImEN2uvSZE4oAzLZq"}
    response = requests.request("GET", url, headers=headers, timeout=(120.0, 300.0))
    if response.status_code != 200:
        add_error_log(f"dify search file fail{response.text}")
    else:
        result = json.loads(response.text)
    return result


def upload_to_dify(DIFY_TOKEN):
    
    
    
    
    
    
    sql = "select * from TB_Research_Attribute where attribute='meta_summary' and create_time<'2024-09-27 12:00:00' and value!='' and value is not null order by create_time limit 100 offset 70"
    files = []
    temp_uuid = []
    attributes = query_dict(sql)
    for i in attributes:
        if i['research_id'] in temp_uuid:
            continue
        temp_uuid.append(i['research_id'])
        sql = """
        select * from TB_Research where uuid=%s limit 1
        """
        research = query_dict(sql, (i['research_id'],))
        if research:
            research[0]['summary'] = i['value']
            files.append(research[0])

    config = CosConfig(Region='ap-beijing', SecretId=SecretId, SecretKey=SecretKey, Token=None,
                       Scheme='https')
    client = CosS3Client(config)
    temp = []
    for file in files:
        if not file['download_status']:
            continue
        if file['source'] in IB_SOURCE and file['file_type'] == 'pdf' and file['parse_status'] == 'parse_ok':
            temp.append(file)
            continue
        if file['source'] == 'quartr' and file['business_type'] == 'slides':
            continue
        if file['source'] == 'quartr' and file['business_type'] == 'audio':
            temp.append(file)
            continue
        if file['source'] == 'quartr' and file['business_type'] == 'report':
            temp.append(file)
        if file['source'] == 'sa' and file['file_type'] == 'html':
            temp.append(file)
        if file['source'] == 'fargo' and file['file_type'] == 'html':
            temp.append(file)
    files = temp
    for file in files:
        try:
            if (file['source'] == 'sa' or file['source'] == 'fargo') and file['file_type'] == 'html':
                embedding_html(file=file, DIFY_TOKEN=DIFY_TOKEN)
                continue
            file_type = "pdf"
            file_extension = "pdf"
            upload_type = "application/pdf"
            if file['source'] == 'quartr' and file['business_type'] == 'audio':
                file_type = "json"
                file_extension = "txt"
                upload_type = "text/plain"
            
            
            
            
            
            
            
            url = "https://ops.fargoinsight.com/console/api/files/upload?source=datasets"
            querystring = {"source": "datasets"}
            path = f"{PATH}{file['uuid']}.{file_type}"
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
                    'file': (file['uuid'] + f".{file_extension}", file_content, upload_type),
                }
                form_data = MultipartEncoder(file_p)  
                headers['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=headers, params=querystring, timeout=500)
                data = json.loads(r.text)
                if not data['id']:
                    add_error_log(f"sync_embedding upload pdf file fail {r.text}")
                    continue
                else:
                    dify_file_id = data['id']
            embedding_res = False
            timestamp = int(file['publish_time'].timestamp())
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            if file['source'] in IB_SOURCE or (
                    file['source'] == 'quartr' and file['business_type'] == 'report'):
                response = client.get_object(
                    Bucket='research1-1328064767',
                    Key=f"{file['uuid']}_{GALPHA_PARSING_VERSION}.json"
                )
                res_json = response['Body'].read(chunk_size=sys.maxsize)
                if res_json:
                    parsing_data = json.loads(res_json)
                    parsing_data = report_resort(parsing_data)
                    upload_data = {}
                    
                    for key in list(parsing_data.keys()):
                        parsing_data[key]['metadata'].pop('attribute', None)
                        parsing_data[key]['metadata'].pop('text', None)
                        upload_data[key] = parsing_data[key]
                        
                    merge_text = f"\n\n Fargo identify title:\n{file['title']}\n\n "
                    summary_attr = split_string_into_chunks(long_string=file['summary'])
                    for i in summary_attr:
                        merge_text = f"{merge_text}\n\n Fargo identify summary:\n{i}\n\n "
                    upload_data['summary'] = {
                        "data": merge_text,
                        "metadata": {
                            "page_id": 0,
                            "uuid": f"{file['uuid']}",
                            "id": f"{file['uuid']}",
                            "location": {}
                        }
                    }
                    embedding_res = dify_embedding(dify_file_id, file['uuid'], upload_data, DIFY_TOKEN, timestamp,
                                                   file['source'], file_extension)

        except Exception as e:
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
            add_error_log(e=e, message=f"Report embedding fail:{str(e)}")

            continue


def embedding_html(file, DIFY_TOKEN):
    try:

        file_type = "html"
        file_extension = "txt"
        upload_type = "text/plain"
        url = "https://ops.fargoinsight.com/console/api/files/upload?source=datasets"
        querystring = {"source": "datasets"}
        path = f"{PATH}{file['uuid']}.{file_type}"
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
                'file': (file['uuid'] + f".{file_extension}", file_content, upload_type),
            }
            form_data = MultipartEncoder(file_p)  
            headers['content-type'] = form_data.content_type
            r = requests.post(url, data=form_data, headers=headers, params=querystring, timeout=500)
            data = json.loads(r.text)
            if not data['id']:
                add_error_log(f"sync_embedding upload pdf file fail {r.text}")
            else:
                dify_file_id = data['id']
        embedding_res = False
        timestamp = int(file['publish_time'].timestamp())
        with open(f"{PATH}{file['uuid']}.{file_type}", 'r', encoding='UTF-8') as f:
            merge_text = f"{html2text.html2text(f.read())}"
            merge_text = f"{merge_text}\n\n Fargo identify title:\n{file['title']}\n\n "
            summary_attr = split_string_into_chunks(long_string=file['summary'])
            for i in summary_attr:
                merge_text = f"{merge_text}\n\n Fargo identify summary:\n{i}\n\n "
            upload_data = {
                f"{file['uuid']}": {
                    "data": merge_text,
                    "metadata": {
                        "page_id": 0,
                        "uuid": f"{file['uuid']}",
                        "id": f"{file['uuid']}",
                        "location": {}
                    }
                }
            }
            embedding_res = dify_embedding(dify_file_id, file['uuid'], upload_data, DIFY_TOKEN, timestamp,
                                           file['source'], file_extension)

    except Exception as e:
        print(e)


def split_string_into_chunks(long_string, chunk_size=1600):
    """
    将长字符串分割成数组，每个元素最多包含chunk_size个字符。

    参数:
    long_string (str): 要分割的长字符串。
    chunk_size (int): 每个元素的最大字符数，默认为2000。

    返回:
    list: 包含分割后字符串的数组。
    """
    return [long_string[i:i + chunk_size] for i in range(0, len(long_string), chunk_size)]


def dify_embedding(upload_id: str, file_id: str, upload_data: dict, dify_token: str, publish_time: int, source: str,
                   file_type: str):
    result = False
    try:

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
            "referer": f"https://ops.fargoinsight.com/datasets/{DIFY_REPOSITORY}/documents/create",
            "Authorization": f"Bearer {dify_token}"}

        response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
        if response.status_code != 200:
            add_error_log(message=f"dify embedding api error:{response.text}")
            return False
        for i in range(0, 60):
            
            res = search_dify_file(file_id=file_id, file_type=file_type)
            if res['total'] == 0:
                continue
            if res['data'][0]['indexing_status'] == "completed":
                return True
            time.sleep(5)
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
        add_error_log(e=e, message=f"dify embedding api fail:{str(e)}")
    return result


def embedding():
    print("sync_embedding")
    DIFY_TOKEN = get_system_variable("dify_token")
    if len(DIFY_TOKEN) > 0:
        DIFY_TOKEN = DIFY_TOKEN[0]["value"]
    
    upload_to_dify(DIFY_TOKEN)


if __name__ == '__main__':
    embedding()
