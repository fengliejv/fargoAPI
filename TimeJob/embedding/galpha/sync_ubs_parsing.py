import json
from datetime import datetime

import requests
from requests_toolbelt import MultipartEncoder

from service.FileBasicService import get_file_basic_need_parse, set_file_basic_attr
from service.GAlphaService import get_consumer_node_time, add_parsing_record, set_sync_node_time
from service.ReportService import add_error_log, add_fatal_log

GALPHA_PARSING_VERSION = "1.1"


def sync_ubs_parsing():
    node_time = get_consumer_node_time(task_name='sync_ubs_parsing', consumer='galpha')
    files = get_file_basic_need_parse(start_time=node_time[0]['node_time'], source='ubs')
    for file in files:
        try:
            set_file_basic_attr(file['uuid'], 'parse_status', 'parsing')
            url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file"
            header = {}
            with open(file=file['local_save_path'], mode='rb') as fis:
                file_content = fis
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
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                    print(f"Report parsing失败")
                    add_fatal_log(f"ubs Report parsing失败")
                else:
                    data = json.loads(r.text)
                    if "parsed_result" not in data:
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                        continue
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                    res = json.loads(data["parsed_result"])
                    res_json = json.dumps(res)
                    add_parsing_record(file_id=file['uuid'], parsing_platform='galpha', req=file_p['file_metadata'],
                                       result=res_json, article_id=file['article_id'], response=r.text,
                                       version=GALPHA_PARSING_VERSION)
                    print(f"success,{file['uuid']}")

                    set_sync_node_time(node_time=file['create_time'], update_time=datetime.now(),
                                       task_name='sync_ubs_parsing')
        except Exception as e:
            print(str(e))
            add_error_log(message=f"ubs Report Parsing失败,报错:{str(e)}")
            continue


if __name__ == '__main__':
    sync_ubs_parsing()
