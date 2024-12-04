import datetime
import json

import requests
from requests_toolbelt import MultipartEncoder

from api.v1.lib.common.utils import report_resort
from api.v1.service.GAlphaService import get_parsing_record, add_parsing_record, set_parsed_file_status, \
    get_parsed_file_status
from api.v1.service.ReportService import get_file_basic_by_file_id, add_fatal_log, get_file_basic_by_time, \
    get_file_basic_not_handle_by_time
from api.v1.service.SystemService import get_system_variable
from lib.Common.my_minio import Bucket

GALPHA_PARSING_VERSION = "1.1"


def parsing_report(file_id):
    print(file_id)
    res = {
        'status': False,
        'err_msg': ""
    }
    DIFY_REPOSITORY = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
    res_json = ""
    parsing_text = ""
    try:
        DIFY_TOKEN = get_system_variable("dify_token")
        if len(DIFY_TOKEN) > 0:
            DIFY_TOKEN = DIFY_TOKEN[0]["value"]
        else:
            res["err_msg"] = "dify token expire"
            return res

        file = get_file_basic_by_file_id(file_id)
        if len(file) == 0:
            res["err_msg"] = "file not found"
            return res
        file = file[0]
        file_content = open(file=file['local_save_path'], mode='rb')
        async_record = get_parsing_record(article_id=file['article_id'], version_id=GALPHA_PARSING_VERSION)
        if len(async_record) > 0:
            res_json = async_record[0]['result']
            if res_json == '':
                
                minio_obj = Bucket()
                res_json = minio_obj.client.get_object("report-parse-result",
                                                       f'{file_id}_{GALPHA_PARSING_VERSION}').data.decode('utf-8')
                del minio_obj
            parsing_data = json.loads(res_json)
            parsing_data = report_resort(parsing_data)
            for key in list(parsing_data.keys()):
                parsing_text = parsing_text + " " + parsing_data[key]['data']
        else:
            url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file"
            header = {}
            file_p = {
                'filename': file['local_save_path'],
                'Content-Disposition': 'form-data;',
                'Content-Type': 'multipart/form-data',
                'file': (file['local_save_path'], file_content, 'multipart/form-data'),
                'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"}'
            }
            form_data = MultipartEncoder(file_p)  
            header['content-type'] = form_data.content_type
            print("parsing...")
            r = requests.post(url, data=form_data, headers=header, timeout=1200)
            print("parsed")
            if r.status_code != 200:
                res["err_msg"] = "Report parsing fail"
                add_fatal_log(f"Report parsing失败file_id{file_id}")
                return res

            data = json.loads(r.text)
            if "parsed_result" not in data:
                print(f"Report parsing null")
                add_fatal_log(f"Report parsing null file_id{file_id}")
                res["err_msg"] = "Report parsing null"
                return res
            result = json.loads(data["parsed_result"])
            res_json = json.dumps(result)
            add_parsing_record(file_id=file['uuid'], parsing_platform='galpha', req=file_p['file_metadata'],
                               result=res_json, article_id=file['article_id'], response=r.text,
                               version=GALPHA_PARSING_VERSION)
            print(f"success add parsed content")

        
        async_record = get_parsing_record(article_id=file['article_id'], version_id=GALPHA_PARSING_VERSION)
        if len(async_record) > 0:
            temp = int.from_bytes(async_record[0]["upload_status"], byteorder='big')
            if not temp:
                url = "https://ops.fargoinsight.com/console/api/files/upload?source=datasets"
                querystring = {"source": "datasets"}
                path = file['local_save_path']
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
                file_p = {
                    'file': (file_id + ".pdf", file_content, 'application/pdf'),
                }
                form_data = MultipartEncoder(file_p)  
                headers['content-type'] = form_data.content_type
                print(f"upload to dify...")
                r = requests.post(url, data=form_data, headers=headers, params=querystring)
                print(f"upload to dify down")
                data = json.loads(r.text)
                if "id" not in data:
                    res["err_msg"] = "file to dify fail"
                    return res
                parsing_data = json.loads(res_json)
                parsing_data = report_resort(parsing_data)
                upload_data = {}
                parsing_text = ""
                for key in list(parsing_data.keys()):
                    upload_data[key] = parsing_data[key]
                    parsing_text = parsing_text + " " + parsing_data[key]['data']
                datas = {
                    "file_id": file['uuid'],
                    "parsing_data": upload_data
                }
                url = f'https://ops.fargoinsight.com/console/api/datasets/{DIFY_REPOSITORY}/documents/custom'

                payload = {
                    "data_source": {
                        "type": "upload_file",
                        "info_list": {
                            "data_source_type": "upload_file",
                            "file_info_list": {"file_ids": [data['id']]}
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
                        "search_method": "semantic_search",
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
                    "Authorization": f"Bearer {DIFY_TOKEN}"}
                print(f"start embedding...")
                response = requests.request("POST", url, json=payload, headers=headers)
                print(f"end embedding")
                if response.status_code == 200:
                    set_parsed_file_status(file_id=file['uuid'])
                    print(f"success{file_id}")
        else:
            res["err_msg"] = "parsed data not found"

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        


    except Exception as e:
        res[
            "err_msg"] = f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
    return res


def get_report_format(ticker, publish_time, source, title, title_en, summary, url):
    p = source
    if source == "ubs":
        p = "瑞银"
    elif source == "gs":
        p = "高盛"
    elif source == "ms":
        p = "摩根士丹利"
    return f"Ticker: {ticker}\n" \
           f"Publish time: {publish_time}\n" \
           f"Source: {p}\n\n" \
           f"{title} {title_en}\n\n" \
           f"{summary}\n\n" \
           f"Link: {url}"


def report_parsing_to_dify(platform):
    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = (datetime.datetime.now() - datetime.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    files = get_file_basic_not_handle_by_time(publish_time_start=start_time, publish_time_end=end_time, source=platform)
    total = len(files)
    count = 0
    for file in files:
        count = count + 1
        print(f"{count}/{total}")
        parsed_file = get_parsed_file_status(file['uuid'])
        if len(parsed_file) > 0:
            parsing_report(file['uuid'])
        else:
            print("already embedding ")


def report_parsing_to_dify_uuid():
    parsing_report("acd9cbde-17d0-11ef-911c-0fe07a410ac4")


if __name__ == "__main__":
    report_parsing_to_dify_uuid()
    
