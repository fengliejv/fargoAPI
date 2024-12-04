import io
import time

import requests
import json
import re

from bs4 import BeautifulSoup

from service.ReportService import get_file_record_by_type, add_fatal_log

CLIENT_ID = 'cioinsight-backend'
CLIENT_SECRET = '0258d90f-fa98-4b16-916c-51c8a38c3a46'
ARTICLE_TABLE = 'native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad'
FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'
API_TOKEN = ''

def sync_file_to_insight_article_page(content: str, summary: str, symbol: str, article_id: str) -> bool:
    try:
        url = "http://ops.fargoinsight.com/v1/completion-messages"
        payload = {
            "inputs": {
                "summary": summary,
                "Content": content
            },
            "response_mode": "blocking",
            "user": "abc-123"
        }
        headers = {
            "Authorization": "Bearer app-PPbq1SZhQL81KZ3gUKyLP1i0",
            "Content-Type": "application/json",
            "content-type": "application/json"
        }
        response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
        if response.status_code != 200:
            return False

        data = json.loads(response.text)
        print("测试" + data['answer'])
        answer = data['answer'].replace('\\n', "").replace("\"", "").strip('`').replace('json', '').strip("\"").replace(
            'plaintext', "").replace(" ", "").strip("\n")
        if answer[0] == "[":
            answer = answer[1:len(answer)]
        if answer[len(answer) - 1] == "]":
            answer = answer[0:len(answer) - 1]
        print(answer)
        qs = answer.split(",")
        for question in qs:
            if not question:
                continue
            url = "http://ops.fargoinsight.com/v1/completion-messages"
            payload = {
                "inputs": {
                    "Question": question,
                    "Content": content
                },
                "response_mode": "blocking",
                "user": "abc-123"
            }
            headers = {
                "Authorization": "Bearer app-Rv280n7rbuWnqLJf9wYm1GWB",
                "Content-Type": "application/json",
                "content-type": "application/json"
            }
            response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
            if response.status_code != 200:
                add_fatal_log(f"定时更新公司报告异常,问题答案chat/生成API失败")
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
                                "6HooE": symbol,
                                "5PN73": article_id,
                                "BufLj": question,
                                "4Rz5j": json.loads(response.text)['answer']
                            }
                        }
                    ]
                },timeout=(120.0,300.0)
            )
            if r.status_code != 200:
                add_fatal_log(f"定时更新公司报告异常,ViewCard表更新失败")
            print(f"成功,article{article_id}")
        return True
    except Exception as e:
        add_fatal_log(f"定时更新公司报告异常,insight更新失败,异常{str(e)}")
        return False

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
            
            content = responseHtml.find_all(
                class_="paywall-full-content hq_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                content = responseHtml.find_all(
                    class_="paywall-full-content hp_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                content = responseHtml.find_all(
                    class_="paywall-full-content hs_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                content = responseHtml.find_all(
                    class_="hq_f flex sa-text-content sa-media-content sa-table-content sa-content-helpers print:block")
            if len(content) <= 0:
                continue
            content =  str(content[0])
            
            stock = doc['file_path'][24:doc['file_path'].rfind("/")]
            summary = json.dumps(data['article']['response']['data']['attributes']['summary'])
            if int(time.mktime(time.strptime(doc['publish_time'], '%Y-%m-%d %H:%M:%S')))>=int(time.mktime(time.strptime("2023-12-27 00:00:00", '%Y-%m-%d %H:%M:%S'))):
                print("报告过新")
                continue
            
            res = sync_file_to_insight_article_page(content=content, summary=summary,
                                                    symbol=com_dict.get(stock), article_id=id)
            if not res:
                print(f'同步失败{id}')
            print(f'同步成功{id}')
        except Exception as e:
            print(str(e))
    return f'success'


if __name__ == '__main__':
    update_all()
