import json
from datetime import datetime

import requests

from service.InsightService import get_report_file_basic_not_sync, get_sync_node_time, set_sync_node_time, \
    get_fargo_insight_file_basic_not_sync, get_sync_record, add_sync_record
from service.ReportService import add_error_log, \
    get_answer_not_sync, add_fatal_log, get_file_basic_not_sync

FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'
ARTICLE_TABLE = 'native-table-BIQ3ClsZkKo2etsQEtkX'  


def sync_ubs_report():
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    node_time = get_sync_node_time(task_name='sync_ubs_report')
    files = get_fargo_insight_file_basic_not_sync(create_time=node_time[0]['node_time'], source='ubs')

    for file in files:
        last_index = file['local_save_path'].rfind("/")
        second_index = file['local_save_path'].rfind("/", 0, last_index)
        third_index = file['local_save_path'].rfind("/", 0, second_index)
        url_path = file['local_save_path'][third_index:len(file['local_save_path'])]
        try:
            async_record = get_sync_record(file['article_id'])
            if len(async_record) > 0:
                continue
            r = requests.post(
                "https://api.glideapp.io/api/function/mutateTables",
                headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                json={
                    "appID": "uNOgjdbeolykCXHBMvi0",
                    "mutations": [
                        {
                            "kind": "add-row-to-table",
                            "tableName": "native-table-BIQ3ClsZkKo2etsQEtkX",
                            "columnValues": {
                                "Name": f"{file['source']}".upper(),
                                "hhkZy": file['original_url'],
                                "Rbr9g": url_path,
                                "rbyUD": f"{file['symbol']}",
                                "4CuPn": f"{file['title']}".replace('"', ''),
                                "lg8Ho": f"{file['uuid']}",
                                "9xwYw": f"{file['article_id']}",
                                "QlBCH": f"{file['create_time']}",
                                "VNyCD": f"{file['publish_time']}"
                            }
                        }
                    ]
                }, timeout=(120.0, 300.0)
            )
            if r.status_code != 200:
                add_fatal_log(f"fail")
            else:
                
                add_sync_record(file_id=file['article_id'])
                
                set_sync_node_time(node_time=file['create_time'], update_time=datetime.now(),
                                                 task_name='sync_ubs_report')
                print(f"success,article{file['uuid']}")
        except Exception as e:
            print(str(e))
            add_error_log(message=f"fail:{str(e)}")


if __name__ == '__main__':
    sync_ubs_report()
