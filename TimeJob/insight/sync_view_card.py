import json
from datetime import datetime

import requests

from service.ReportService import add_error_log, \
    get_answer_not_sync, add_fatal_log

FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'


def sync_view_card():
    
    r = requests.post(
        "https://api.glideapp.io/api/function/queryTables",
        headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
        json={
            "appID": "uNOgjdbeolykCXHBMvi0",
            "queries": [
                {
                    "tableName": "native-table-1DpKeFUWBJvhPpMxHtcF",
                    "utc": True
                }
            ]
        }
    )
    latest_time = datetime.strptime("2023-12-30 00:00:00", "%Y-%m-%d %H:%M:%S")
    json_data = json.loads(r.text)[0]['rows']
    for r in json_data:
        if "fIP2S" in r.keys():
            t = datetime.strptime(r['fIP2S'], "%Y-%m-%dT%H:%M:%S.%fZ")
            if t > latest_time:
                latest_time = t
    files = get_answer_not_sync(latest_time=latest_time)
    for file in files:
        try:
            if str(file['question'])=="":
                continue
            r = requests.post(
                "https://api.glideapp.io/api/function/mutateTables",
                headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                json={
                    "appID": "uNOgjdbeolykCXHBMvi0",
                    "mutations": [
                        {
                            "kind": "add-row-to-table",
                            "tableName": "native-table-1DpKeFUWBJvhPpMxHtcF",
                            "columnValues": {
                                "6HooE": file['symbol'],
                                "5PN73": file['article_id'],
                                "z13cv": file['uuid'],
                                "BufLj": file['question'].strip(),
                                "4Rz5j": file['answer'].strip(),
                                "4ynD8": file['author_head'],
                                "lomyp": file['author'],
                                "0GEY0": file['rating'],
                                "Udlp5": f"{file['publish_time']}",
                                "W9CJ0": file['title'].replace('"',''),
                                "fIP2S": f"{file['create_time']}"
                            }
                        }
                    ]
                }, timeout=(120.0, 300.0)
            )
            if r.status_code != 200:
                add_fatal_log(f"fail")
        except Exception as e:
            add_error_log(message=f"fail:{str(e)}")


if __name__ == '__main__':
    sync_view_card()
