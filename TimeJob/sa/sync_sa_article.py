import io

import requests
import json
import re

from bs4 import BeautifulSoup

from service.ReportService import get_file_record_by_type

CLIENT_ID = 'cioinsight-backend'
CLIENT_SECRET = '0258d90f-fa98-4b16-916c-51c8a38c3a46'
ARTICLE_TABLE = 'native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad'
FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'
API_TOKEN = ''


def update_all():
    update_count = 0
    new_count = 0
    
    docs = get_file_record_by_type(type='seekingalpha.com')
    doc_dict=[]
    
    file = open("/home/ibagents/bugs/static/stock-company.csv", "r")  
    content = file.readlines()  
    com_dict=dict()
    for line in content:  
        line=line.strip('\n')
        temp=line.split(",")
        if temp[1]:
            com_dict.setdefault(temp[1],temp[0])
    for doc in docs:
        try:
            id = json.loads(doc['attribute'])['id']
            if id in doc_dict:
                continue
            else:
                doc_dict.append(id)
            file = io.open(doc['file_path'], mode='r', encoding='utf-8')
            content = file.read()
            file.close()
            
            responseHtml = BeautifulSoup(content, 'html.parser')
            script_tags = responseHtml.find_all('script')
            script_tag = script_tags[4]  
            match = str(script_tag.string)[18:len(script_tag.string) - 1]
            data = json.loads(match)
            
            content = responseHtml.find_all(class_="paywall-full-content hp_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")

            content =  str(content[0])
            
            stock = doc['file_path'][24:doc['file_path'].rfind("/")]
            summary = json.dumps(data['article']['response']['data']['attributes']['summary'])
            
            r = requests.post(
                "https://api.glideapp.io/api/function/queryTables",
                headers={"Authorization": f'Bearer {FARGO_INSIGHT_KEY}'},
                json={
                    "appID":
                        "uNOgjdbeolykCXHBMvi0",
                    "queries": [{
                        "sql":
                            f"SELECT * from \"native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad\" where \"n99aj\"={id} and \"m9BiN\"='SA'"
                    }]
                })
            if len(json.loads(r.text)[0]['rows']) > 0:
                continue
            lang = 'en_US'
            r = requests.post(
                "https://api.glideapp.io/api/function/mutateTables",
                headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                json={
                    "appID":
                        "uNOgjdbeolykCXHBMvi0",
                    "mutations": [{
                        "kind": "add-row-to-table",
                        "tableName": f"{ARTICLE_TABLE}",
                        "columnValues": {
                            "n99aj": id,
                            "wG4ZB": com_dict.get(stock),
                            "Jww8r": doc['title'],
                            "zpVLQ": content,
                            "ZicAt": summary,
                            "LppoE": doc["publish_time"],
                            "oCPF0": lang,
                            "m9BiN": "SA",
                        }
                    }]
                })
            if r.status_code != 200:
                pass
            new_count += 1
            print(f'新增{id}')
        except Exception as e:
            print(str(e))
    return f'更新{update_count}条，新增{new_count}条。'


if __name__ == '__main__':
    update_all()
