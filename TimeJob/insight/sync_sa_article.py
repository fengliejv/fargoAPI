import json
from datetime import datetime

import requests

from service.InsightService import set_sync_node_time, get_sync_node_time
from service.ReportService import add_error_log, \
    get_answer_not_sync, add_fatal_log, get_file_basic_not_sync

FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'
ARTICLE_TABLE = 'native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad'


def sync_sa_article():
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    node_time = get_sync_node_time(task_name='sync_sa_article')
    files = get_file_basic_not_sync(latest_time=node_time[0]["node_time"])
    summary = {}
    distinct_files = []
    for file in files:
        if file['uuid'] in summary.keys():
            summary[file['uuid']].append(file['summary'])
        else:
            summary[file['uuid']] = [file['summary']]
            distinct_files.append(file)

    for file in distinct_files:
        try:
            try:
                content = ''.join(
                    chr(int(file['content'][i:i + 8], 2)) for i in range(0, len(file['content']), 8))
            except Exception as e:
                content = ""
                pass
            
            update_node = set_sync_node_time(node_time=file['create_time'], update_time=datetime.now(),
                                             task_name='sync_sa_article')
            if not update_node:
                break
            r = requests.post(
                "https://api.glideapp.io/api/function/mutateTables",
                headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                json={
                    "appID": "uNOgjdbeolykCXHBMvi0",
                    "mutations": [
                        {
                            "kind": "add-row-to-table",
                            "tableName": "native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad",
                            "columnValues": {
                                "n99aj": file['article_id'],
                                "yhBeu": file['uuid'],
                                "wG4ZB": file['symbol'],
                                "Jww8r": file['title'].replace('"',''),
                                "zpVLQ": content,
                                "ZicAt": json.dumps(summary[file['uuid']]),
                                "LppoE": f'{file["publish_time"]}',
                                "oCPF0": file["lang"],
                                "m9BiN": file["source"],
                                "iViF8": f"{file['create_time']}"
                            }
                        }
                    ]
                }, timeout=(120.0, 300.0)
            )
            if r.status_code != 200:
                add_fatal_log(f"fail")

            print(f"success,article{file['uuid']}")
        except Exception as e:
            print(str(e))
            add_error_log(message=f"fail:{str(e)}")


if __name__ == '__main__':
    sync_sa_article()
