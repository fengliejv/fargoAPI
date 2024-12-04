import datetime
import json
import time

import requests
from minio import S3Error
from requests_toolbelt import MultipartEncoder
from api.v1.lib.common.utils import report_resort
from lib.Common.my_minio import Bucket
from lib.Common.mysqlsingle import query_dict
from service.FileBasicService import get_file_basic_need_embedding, set_file_basic_attr, set_same_file_embedding_status
from service.ReportService import add_error_log
from service.SystemService import get_system_variable

GALPHA_PARSING_VERSION = "1.1"
DIFY_REPOSITORY = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
DATASET_ID = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"


def search_dify_file(file_id):
    result = False
    
    url = f"http://ops.fargoinsight.com/v1/datasets/{DATASET_ID}/documents?keyword={file_id}.pdf"
    headers = {"Authorization": "Bearer dataset-Xdljx2aImEN2uvSZE4oAzLZq"}
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        add_error_log(f"dify search file fail{response.text}")
    else:
        result = json.loads(response.text)
    return result


def upload_to_dify(DIFY_TOKEN):
    
    start_time = '2024-1-01 00:00:00'
    sql = f"select a.article_id,a.uuid,a.source,a.local_save_path,a.create_time,a.publish_time,b.version from TB_File_Basic as a inner join " \
          f"TB_File_Parsing as b on a.uuid=b.file_id where a.publish_time>'{start_time}' " \
          f"and (embedding_status='pending' or embedding_status is null) order by a.publish_time"
    files = query_dict(sql)
    article_id = []
    print(len(files))
    for file in files:
        try:
            if file['article_id'] in article_id:
                print(f"same {file['article_id']} {file['uuid']} {file['publish_time']} {file['local_save_path']}")
                continue
            else:
                print(f"{file['article_id']} {file['uuid']} {file['publish_time']} {file['local_save_path']}")
            
            res = search_dify_file(file['uuid'])
            if res['total'] > 0:
                dify_file_id = res['data'][0]['id']
                if res['data'][0]['indexing_status'] == "completed":
                    set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_ok')
                    continue
            else:
                url = "https://ops.fargoinsight.com/console/api/files/upload?source=datasets"
                querystring = {"source": "datasets"}
                path = file['local_save_path']
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
                        set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
                        add_error_log(f"sync_embedding upload pdf file fail {r.text}")
                        continue
                    else:
                        dify_file_id = data['id']
            try:
                minio_obj = Bucket()
                res_json = minio_obj.client.get_object("report-parse-result",
                                                       f'{file["uuid"]}_{file["version"]}').data.decode(
                    'utf-8')
                del minio_obj
            except S3Error as e:
                add_error_log(f"get object {file['uuid']} fail {e}")
                set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
                continue
            if res_json:
                embedding_res = dify_embedding(dify_file_id, file['uuid'], json.loads(res_json), DIFY_TOKEN)
                if embedding_res:
                    set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_ok')
                    if set_same_file_embedding_status(file['article_id'], file['source']):
                        article_id.append(file['article_id'])
                else:
                    set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
            else:
                set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
        except Exception as e:
            print(str(e))
            add_error_log(e=e, message=f"Report embedding fail:{str(e)}")
            set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
            continue


def dify_embedding(upload_id: str, file_id: str, parsing_data: dict, dify_token: str):
    result = False
    try:
        parsing_data = report_resort(parsing_data)
        upload_data = {}
        parsing_text = ""
        for key in list(parsing_data.keys()):
            upload_data[key] = parsing_data[key]
            parsing_text = parsing_text + " " + parsing_data[key]['data']
        datas = {
            "file_id": file_id,
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
                "top_k": 3,
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
            add_error_log(message=f"dify embedding api error:{response.text}")
            return False
        for i in range(0, 60):
            
            res = search_dify_file(file_id=file_id)
            if res['total'] == 0:
                continue
            if res['data'][0]['indexing_status'] == "completed":
                return True
            time.sleep(5)
    except Exception as e:
        print(str(e))
        add_error_log(e=e, message=f"dify embedding api fail:{str(e)}")
    return result


def sync_embedding_all():
    print("sync_embedding")
    DIFY_TOKEN = get_system_variable("dify_token")
    if len(DIFY_TOKEN) > 0:
        DIFY_TOKEN = DIFY_TOKEN[0]["value"]
    
    upload_to_dify(DIFY_TOKEN)


if __name__ == '__main__':
    sync_embedding_all()
