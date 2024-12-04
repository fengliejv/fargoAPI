import json
import os
import time
import re

from pip._vendor import requests
from pymysql.converters import escape_string

from service.ReportService import *

UBS = "https://neo.ubs.com/"
PATH = "/home/ibagents/files/ubs"
YEAR22_START_TIME = '2022-01-01 00:00:00'
YEAR22_END_TIME = '2023-01-01 00:00:00'
YEAR22_START_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_START_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
YEAR22_END_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_END_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
NOW_TIMESTRP = int(time.time())
UBS_PDF_DOWNLOAD_URL = f'{UBS}api/super-grid-provider-research/v1/document/'


def downloadImg(url):
    try:
        response = requests.get(UBS + url)
        if response.status_code == 200:
            index = url.rfind("/")
            dir_path = PATH + url[0:index]
            print(dir_path)
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)
            save_path = PATH + url
            with open(save_path, 'wb') as image_file:
                image_file.write(response.content)
            add_info_log(message=f'图片下载成功 {url}')
        else:
            add_error_log(message=f'图片下载失败 {url}')
    except Exception as e:
        add_fatal_log(message=f"下载图片{url}时发生错误:{str(e)}")


def downloadPdf(file_url, local_file_path, data_row, header):
    time.sleep(1)
    try:
        
        response = requests.get(url=file_url, json=data_row, headers=header)
        
        if response.status_code == 200:
            
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            add_info_log(message=f'文件下载成功 {file_url}')
            print(f'文件下载成功 {file_url}')
            
            return True
        else:
            add_error_log(message=f"文件下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        add_fatal_log(message=f"下载文件时发生错误:{str(e)}")


def downloadHtml(file_url, local_file_path, data_row, header):
    time.sleep(1)
    try:
        
        response = requests.post(url=file_url, json=data_row, headers=header)
        
        if response.status_code == 200:
            pattern = r'<img\s+src="([^"]+\.png)"'
            urls = re.findall(pattern, response.text)
            res_save = response.text
            for url in urls:
                if 'https://' not in url:
                    path_change = url.replace(":", "_")
                    res_save = res_save.replace(url, f"..{path_change}")
                    downloadImg(url)
            
            with open(local_file_path, 'wb') as file:
                file.write(bytes(res_save, encoding='utf-8'))
            add_info_log(message=f"网页下载成功{local_file_path}")
        else:
            add_error_log(message=f"网页下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        print("下载网页时发生错误:", str(e))
        add_fatal_log(message=f"下载网页时发生错误:{str(e)}")


def get_ubs_report():
    file = open("company.txt", "r")  
    content = file.readlines()  
    company_list=[]
    for line in content:  
        line=line.strip('\n')
        company_list.append(line.split(","))
    print(company_list)
    skip = 0
    count = 0
    for item in company_list:
        count = count + 1
        if count < skip:
            print(f"skip:{item[0]}")
            continue
        page = 0
        while page < 10000:
            
            doc_publish_time = NOW_TIMESTRP
            if not item[0]:
                break
            print(F"爬取{item[0]},page:{page}")
            add_info_log(message=f"爬取{item[0]},page:{page}")
            url = f"https://neo.ubs.com/api/search/v2/research-stream-advanced?q=*&limit=20&offset={page*20}"
            payload = {
                "filters": {"andFilters": {
                    "focus": ["company"],
                    "ric": [item[1]]
                }},
                "notFilters": {"researchTypeCode": ["PT.PACKAGE"]}
            }
            headers = {
                "ADRUM": "isAjax:true",
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "keep-alive",
                "Content-Type": "application/json",
                "Origin": "https://neo.ubs.com",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "x-client-component-id": "FedPCC_pcc-client-stream-panel",
                "x-csrf-token": "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJQQ0M4YTlhYzBiNThhZGVkZjBhMDE4YWRlZjVhZjVlMDE0NiIsImlhdCI6MTcwMzE1NDgwNiwiZXhwIjoxNzAzMjQxMjA2fQ.qUIL-jN0Zko04HzRgpp7OuAd6Mko2z-WmgFl91wleMEZvXKNZzBYbb-drJucOXqZx-RVBmubiJ7xsC_Onk5kXw",
                "x-original-client-component-id": "FedPCC_pcc-client-stream-panel",
                "Cookie": ""
            }
            try:
                reSp = requests.request("POST", url, json=payload, headers=headers)
                json_data = json.loads(reSp.text)
                documents = json_data['categories']['research']['results']
                dir_name = PATH + "/" + item[0].replace(".", "").replace(" ", "").replace("&", "")
                if not os.path.isdir(dir_name):
                    os.makedirs(dir_name)
                for doc in documents:
                    if not doc['pubDate']:
                        continue
                    doc_publish_time = int(time.mktime(time.strptime(doc['pubDate'], '%Y-%m-%dT%H:%M:%S.%f'))) * 1000
                    if doc_publish_time < YEAR22_START_TIME_TIMESTRP:
                        break
                    if doc['neoUrlPath']:
                        name = doc['neoUrlPath'][doc['neoUrlPath'].rfind("/") + 1:len(doc['neoUrlPath'])]
                        if '.zip' in name:
                            continue
                        res = downloadPdf(UBS_PDF_DOWNLOAD_URL + name, dir_name + '/' + name, payload, headers)
                        if res:
                            add_file_record(type="pdf",
                                            file_path=dir_name + '/' + name,
                                            title=doc['title'],
                                            source=UBS_PDF_DOWNLOAD_URL + name,
                                            attribute=escape_string(json.dumps(doc)),
                                            publish_time=datetime.datetime.strptime(doc['pubDate'], '%Y-%m-%dT%H:%M:%S.%f'))
                    
                    
                    
                if doc_publish_time < YEAR22_START_TIME_TIMESTRP:
                    page = 10001
                else:
                    page = page + 1
            except Exception as e:
                print(f"下载公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")
                add_fatal_log(
                    message=f"下载公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")
