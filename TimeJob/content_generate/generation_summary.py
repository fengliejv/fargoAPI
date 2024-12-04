import json
import requests
import deepl

from api.v1.config import Config
from lib.Common.my_minio import Bucket
from service.ParsedService import add_summary, get_not_generation_summary_file, get_not_generation_summary_file_title
from service.ReportService import add_error_log, add_fatal_log
from service.SystemService import get_system_variable


def sync_generation_summary():
    try:
        content = ""
        files = get_not_generation_summary_file()
        for file in files:
            
            if file['result'] == '':
                
                minio_obj = Bucket()
                file['result'] = minio_obj.client.get_object("report-parse-result",
                                                             f'{file["file_id"]}_{file["version"]}').data.decode(
                    'utf-8')
                del minio_obj
            res_dict = json.loads(file['result'])
            end = False
            for key in res_dict:
                for rule in Config.UBS_FILTER_RULERS:
                    if rule in res_dict[key]['data']:
                        end = True
                        break
                if end: break
                if not res_dict[key]['metadata']["data_type"] == "figure":
                    content = content + res_dict[key]["data"]
            title = get_not_generation_summary_file_title(file['file_id'])
            if content == "":
                return False
            url = "http://ops.fargoinsight.com/v1/completion-messages"
            payload = {
                "inputs": {
                    "title": title,
                    "content": content
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
                add_error_log(f"fail:{response.text}")
            answer = json.loads(response.text)['answer']
            res = add_summary(file_id=file['file_id'], summary=answer, lang="en")
            if res:
                print(f"en摘要插入成功，摘要：{answer}")
            
            deepl_glossary_id = get_system_variable(key="deepl_glossary_id")
            deepl_key = get_system_variable(key="deepl_key")
            if not (deepl_glossary_id and deepl_key):
                return

            translator = deepl.Translator(deepl_key[0]["value"])
            translate_res = translator.translate_text(text=answer, glossary=deepl_glossary_id[0]["value"],
                                                      target_lang="ZH", source_lang="EN")
            res = add_summary(file_id=file['file_id'], summary=translate_res.text, lang="zh-CN")
            if res:
                print(f"cn摘要插入成功，摘要：{answer}")

    except Exception as e:
        add_error_log(message=f"生成摘要失败,报错:{str(e)}")


if __name__ == '__main__':
    sync_generation_summary()
