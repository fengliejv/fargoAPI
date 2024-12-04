import json
import os
import time
import re
from pip._vendor import requests

from TimeJob.scrap.ubs_cookies_get import ubs_get_cookies
from lib.Common.files import downloadPdf, downloadHtml
from service.ReportService import *
from service.tempService import add_upload_record, get_upload_rescord, add_parsing_record, get_parsing_rescord
from temp.get_token import encode_url


def upload():
    file = open("/home/ibagents/bugs/static/files.txt", "r")
    file_content = file.readlines()
    file_path_list = []
    for line in file_content:
        line = line.strip("\n")
        file_path_list.append(line)
    file.close()
    upload_url = encode_url(
        "https://saas.pdflux.com/api/v1/saas/upload?user=FargoWealth&ocr=true", 'pdflux',
        'EQfwGxU6L9m4')
    for url in file_path_list:
        
        with open(url, 'rb') as file:
            files = {'file': file}
            res = requests.post(upload_url, files=files)
            
            add_upload_record(request_url=upload_url, file_path=url, res=res.text)


def parsing():
    data = get_upload_rescord()

    for i in data:
        try:
            file_id = json.loads(i['res'])["data"]['uuid']
            
            parsing_url = encode_url(
                f'https://saas.pdflux.com/api/v1/saas/document/{file_id}/pdftables?user=FargoWealth',
                'pdflux',
                'EQfwGxU6L9m4')
            res = requests.get(parsing_url)
            
            add_parsing_record(req=parsing_url, result=escape_string(res.text), parsing_platform='chatdoc',
                               file_id=file_id,
                               article_id=i['file_path'])
            print(i['file_path'])
        except Exception as e:
            print(f"报错:{str(e)}")


def sync_to_dify():
    data = get_parsing_rescord()
    for i in data:
        try:
            text = ""
            result = json.loads(i['result'])
            pdf_elements = result["pdf_elements"]
            syllabus = {}
            syllabus_text = ""
            for elements in pdf_elements:
                for element in elements["elements"]:
                    if element["element_type"] == "paragraphs":
                        if element["syllabus"] == -1:
                            text = text + "
                        else:
                            if str(element["syllabus"]) in syllabus:
                                syllabus[str(element["syllabus"])] = syllabus[str(element["syllabus"])] + ".  " + element[
                                    "text"]
                            else:
                                syllabus[str(element["syllabus"])] = element["text"]
                    if element["element_type"] == "tables":
                        table = f'This is a table about {element["title"]}.' \
                                f'Express using "Row_Column: Value",use "|" as a delimiter ,as follows:'
                        table_value = ""
                        for cell in element["cells"]:
                            table_value = table_value + cell + ":" + element["cells"][cell]['value'] + "|"
                        table = table + table_value + "."
                        table = table + "The merging of cells is as follows:"
                        for merge in element["merged"]:
                            m_i = ""
                            for m in merge:
                                m_i = f"{m[0]}_{m[1]},{m_i}"
                            table = table + m_i + "merged.\n"

            for key in syllabus:
                syllabus_text = syllabus_text + "
            text = text + syllabus_text
            url = f"http://ops.fargoinsight.com/v1/datasets/6f90fc40-b39b-4ceb-8a12-4907800e3ca6/document/create_by_text"

            payload = {
                "name": f"{result['document'][0]['filename']}",
                "text": text,
                "indexing_technique": "high_quality",
                "process_rule": {
                    "rules": {
                        "pre_processing_rules": [
                            {
                                "id": "remove_extra_spaces",
                                "enabled": True
                            },
                            {
                                "id": "remove_urls_emails",
                                "enabled": True
                            }
                        ],
                        "segmentation": {
                            "separator": "
                            "max_tokens": 500
                        }
                    },
                    "mode": "custom"
                }
            }
            headers = {
                "Authorization": "Bearer dataset-Xdljx2aImEN2uvSZE4oAzLZq",
                "Content-Type": "application/json",
                "content-type": "application/json"
            }

            response = requests.request("POST", url, json=payload, headers=headers)
            print(response.text)
        except Exception as e:
            print(f"报错:{str(e)}")


if __name__ == '__main__':
    
    
    sync_to_dify()
