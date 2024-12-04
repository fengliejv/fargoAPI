import json
import os
import sys

import deepl
import requests
from requests_toolbelt import MultipartEncoder

import config
from api.v1.config import Config
from api.v1.service.ParsedService import set_file_basic_status
from api.v1.service.ReportService import get_parsed_summary_lang, get_title_lang, add_title
from api.v1.service.GAlphaService import get_parsing_record, add_parsing_record, set_parsed_file_status
from api.v1.service.ParsedService import get_not_generation_summary_file_title, add_summary
from api.v1.service.ReportService import get_file_basic_by_file_id, add_fatal_log, add_error_log
from api.v1.service.SystemService import get_system_variable
from lib.Common.my_minio import Bucket
from service.ReportService import get_file_basic_not_handle, add_info_log

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def parsing_report(file_id):
    GALPHA_PARSING_VERSION = "1.1"
    DIFY_REPOSITORY = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
    res = {
        'status': False,
        'err_msg': ""
    }
    res_json = ""
    parsing_text = ""
    try:
        DIFY_TOKEN = get_system_variable("dify_token")
        if len(DIFY_TOKEN) > 0:
            DIFY_TOKEN = DIFY_TOKEN[0]["value"]
        else:
            res.err_msg = "dify token expire"
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
            end = False
            for key in list(parsing_data.keys()):
                for rule in Config.UBS_FILTER_RULERS:
                    if rule in parsing_data[key]['data']:
                        end = True
                        break
                if end: break
                if not parsing_data[key]['metadata']["data_type"] == "figure":
                    parsing_text = parsing_text + parsing_data[key]['data']
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
            r = requests.post(url, data=form_data, headers=header, timeout=300)

            if r.status_code != 200:
                res["err_msg"] = "Report parsing fail"
                add_fatal_log(f"Report parsing fail")
                return res

            data = json.loads(r.text)
            if "parsed_result" not in data:
                res["err_msg"] = "Report parsing null"
                return res
            result = json.loads(data["parsed_result"])
            res_json = json.dumps(result)
            add_parsing_record(file_id=file['uuid'], parsing_platform='galpha', req=file_p['file_metadata'],
                               result=res_json, article_id=file['article_id'], response=r.text,
                               version=GALPHA_PARSING_VERSION)
            print(f"success,{file['uuid']}")

        
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
                    'file': (path[path.rfind("/") + 1:len(path)], file_content, 'application/pdf'),
                }
                form_data = MultipartEncoder(file_p)  
                headers['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=headers, params=querystring)
                data = json.loads(r.text)
                if "id" not in data:
                    res["err_msg"] = "file to dify fail"
                    return res
                parsing_data = json.loads(res_json)
                parsing_text = ""
                upload_data = {}
                end = False
                for key in list(parsing_data.keys()):
                    for rule in Config.UBS_FILTER_RULERS:
                        if rule in parsing_data[key]['data']:
                            end = True
                            break
                    if end: break
                    if parsing_data[key]['metadata']["data_type"] == "figure":
                        del parsing_data[key]
                    else:
                        upload_data[key] = parsing_data[key]
                        parsing_text = parsing_text + parsing_data[key]['data']
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

                response = requests.request("POST", url, json=payload, headers=headers)

                print(response.text)
                if response.status_code == 200:
                    set_parsed_file_status(file_id=file['uuid'])

        
        
        en_summary = get_parsed_summary_lang(file_id=file['uuid'], lang="en-US")
        answer = ""
        deepl_glossary_id = get_system_variable(key="deepl_glossary_id")
        deepl_key = get_system_variable(key="deepl_key")
        if not (deepl_glossary_id and deepl_key):
            res["err_msg"] = "deepl_glossary_id wrong"
            return res
        translator = deepl.Translator(deepl_key[0]["value"])
        if len(en_summary) == 0:
            title = get_not_generation_summary_file_title(file['uuid'])
            if len(title) > 0:
                title = title[0]["title"]
            else:
                title = " "
            url = "http://ops.fargoinsight.com/v1/completion-messages"
            payload = {
                "inputs": {
                    "title": title,
                    "content": parsing_text
                },
                "response_mode": "blocking",
                "user": "abc-123"
            }
            headers = {
                "Authorization": "Bearer app-sM0wISUKku5VqVmV9cyG09jF",
                "Content-Type": "application/json",
                "content-type": "application/json"
            }
            response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
            if response.status_code != 200:
                add_error_log(f"fail {response.text}")
            answer = json.loads(response.text)['answer']
            add_res = add_summary(file_id=file['uuid'], summary=answer, lang="en-US")
            if add_res:
                print(f"en摘要插入成功，摘要：{answer}")
            

            translate_res = translator.translate_text(text=answer, glossary=deepl_glossary_id[0]["value"],
                                                      target_lang="ZH", source_lang="EN")
            add_res = add_summary(file_id=file['uuid'], summary=translate_res.text, lang="zh-CN")
            if add_res:
                print(f"cn摘要插入成功，摘要：{answer}")
        else:
            answer = en_summary[0]['summary']

        cn_summary = get_parsed_summary_lang(file_id=file['uuid'], lang="zh-CN")
        if len(cn_summary) == 0:
            translate_res = translator.translate_text(text=answer, glossary=deepl_glossary_id[0]["value"],
                                                      target_lang="ZH", source_lang="EN")
            add_res = add_summary(file_id=file['uuid'], summary=translate_res.text, lang="zh-CN")
            if add_res:
                print(f"cn摘要插入成功，摘要：{translate_res}")

        
        cn_title = get_title_lang(file_id=file['uuid'], lang="zh-CN")
        if len(cn_title) == 0:
            translate_res = translator.translate_text(text=file['title'], glossary=deepl_glossary_id[0]["value"],
                                                      target_lang="ZH", source_lang="EN")
            add_res = add_title(file_id=file['uuid'], title=translate_res.text, lang="zh-CN")
            if add_res:
                print(f"en 标题成功，摘要：{translate_res}")
        status = set_file_basic_status(file_id=file['uuid'], status=True)
        if status or int.from_bytes(file['handle_status'], byteorder='big'):
            res['status'] = True
    except Exception as e:
        res[
            "err_msg"] = f'{str(e)}file:{e.__traceback__.tb_frame.f_globals["__file__"]}line:{e.__traceback__.tb_lineno}'
    return res


if __name__ == '__main__':
    
    files = get_file_basic_not_handle()
    for f in files:
        res = parsing_report(f['uuid'])
        print(res)
        if res["status"]:
            add_info_log(f"handle file {f['uuid']} success。{json.dumps(res)}")
        else:
            add_error_log(f"handle{f['uuid']}success。{json.dumps(res)}")
