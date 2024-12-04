import json
import os
import time
import re
from pip._vendor import requests

from TimeJob.scrap.ms_cookies_get import ms_get_cookies
from lib.Common.files import downloadPdf, downloadHtml
from service.ReportService import *

SOURCE = 'ms'
MS = "https://ny.matrix.ms.com"
PATH = f"/home/ibagents/files/{SOURCE}"
YEAR22_START_TIME = '2022-01-01 00:00:00'
YEAR22_END_TIME = '2023-01-01 00:00:00'
YEAR22_START_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_START_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
YEAR22_END_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_END_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
NOW_TIMESTRP = int(time.time())
MS_PDF_DOWNLOAD_URL = f'{MS}/eqr/article/webapp/services/published/rendition/pdf'


def get_ms_report():
    cookies = ms_get_cookies()
    if cookies == "":
        print("获取ms Cookies失败！")
        add_error_log(message=f"获取ms Cookies失败！")
        return
    print(cookies)
    
    get_from_symbol(cookies)
    get_from_sub(cookies)


def get_from_symbol(cookies):
    company_list = get_all_company_code(type="ms")
    print(len(company_list))
    print(f"{SOURCE},time:{datetime.datetime.now()}")
    add_info_log(message=f"scrap {SOURCE},time:{datetime.datetime.now()}")
    skip = 0
    count = 0
    for item in company_list:
        count = count + 1
        if count < skip or len(item[1]) == 0:
            print(f"skip:{item[0]}")
            continue
        page = 1
        
        doc_publish_time = NOW_TIMESTRP
        if not item[0]:
            break
        print(F"爬取{item[0]},page:{page}")
        newest_article = get_article_newest_time_by_symbol(
            platform=SOURCE, symbol=item[2])
        if len(newest_article) <= 0:
            newest_time = "2024-01-01 00:00:00"
        else:
            newest_time = newest_article[0]['publish_time'][0:19]
        print(newest_time)
        url = f"https://ny.matrix.ms.com/eqr/research/webapp/rlservices/search/v2/composite.json"
        while page < 10:
            payload = {
                "search": f"((company=={item[1]}));(productsubtypecode!=StandaloneRiskReward);(aclassl1==Equity)",
                "sort": "d",
                "noSearch": False,
                "gn": False,
                "didyoumean": False,
                "lang": "en,zh",
                "prefLang": "zh",
                "countMode": "narrow",
                "showcard": False,
                "queryID": "15a6c8e0-1d32-4741-9aca-c37e92f10476",
                "userJourneyId": "f1be3c27-41e8-4424-a943-aea40e914a83",
                "size": 10,
                "page": page
            }
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "keep-alive",
                "Content-Type": "application/json",
                "Origin": "https://ny.matrix.ms.com",
                "Referer": "https://ny.matrix.ms.com/eqr/research/ui/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Cookie": f"{cookies}",
                "content-type": "application/json"
            }
            try:
                reSp = requests.request("POST", url, json=payload, headers=headers, timeout=(30.0, 60.0))
                json_data = json.loads(reSp.text)
                documents = json_data['rcsSearchResponse']
                if 'docs' in documents:
                    documents = json_data['rcsSearchResponse']['docs']
                else:
                    break
                dir_name = PATH + "/" + item[0].replace(".", "").replace(" ", "").replace("&", "")
                if not os.path.isdir(dir_name):
                    os.makedirs(dir_name, mode=0o777, exist_ok=False)
                for doc in documents:
                    if not doc['pd']:
                        continue
                    doc_publish_time = int(time.mktime(time.strptime(doc['pd'][0:19], '%Y-%m-%dT%H:%M:%S'))) * 1000
                    if doc_publish_time <= int(time.mktime(time.strptime(newest_time, '%Y-%m-%d %H:%M:%S'))) * 1000:
                        page = 11
                        break
                    if 'pdf' not in doc['dt']:
                        continue
                    
                    url = "https://ny.matrix.ms.com/eqr/article/webapp/services/published/article/frontmatter"
                    querystring = {"uuid": f"{doc['id']}"}
                    response = requests.request("GET", url, headers=headers, params=querystring)
                    response_json = json.loads(response.text)
                    pdfRenditionUrl = response_json['frontMatter']['pdfRenditionUrl']

                    if pdfRenditionUrl:
                        name = pdfRenditionUrl[pdfRenditionUrl.find("pdf") + 4:pdfRenditionUrl.rfind("pdf") + 3]
                        if '.zip' in name:
                            continue
                        res = downloadPdf(MS + pdfRenditionUrl, dir_name + '/' + name, payload,
                                          headers)
                        if res:
                            add_file_record(type="pdf",
                                            file_path=dir_name + '/' + name,
                                            title=doc['hl'],
                                            source=MS + pdfRenditionUrl,
                                            attribute=escape_string(json.dumps(doc)),
                                            publish_time=datetime.datetime.strptime(doc['pd'][0:19],
                                                                                    '%Y-%m-%dT%H:%M:%S'),
                                            symbol=item[2])
                    
                    
                    
                if doc_publish_time < YEAR22_START_TIME_TIMESTRP:
                    page = 11
                else:
                    page = page + 1
            except Exception as e:
                print(
                    f'下载报告异常公司名：{item[0]},公司code:{item[1]}'
                    f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
                add_fatal_log(
                    message=f'下载报告异常公司名：{item[0]},公司code:{item[1]}'
                            f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
                page = 11



def get_from_sub(cookies):
    page = 1
    
    doc_publish_time = NOW_TIMESTRP
    print(f"爬取{SOURCE} sub")
    newest_article = get_article_newest_time_sub(platform=SOURCE)
    if len(newest_article) <= 0:
        newest_time = "2024-01-01 00:00:00"
    else:
        newest_time = newest_article[0]['publish_time'][0:19]
    print(newest_time)
    url = "https://ny.matrix.ms.com/eqr/research/webapp/rlservices/activityfeedService/filterUserActivityFeed"
    while page < 5:
        payload = {
            "appendFiql": "(reporttype!=Model)",
            "page": page,
            "size": 100,
            "lang": "en,zh",
            "prefLang": "zh",
            "gn": False,
            "noSearch": False,
            "entType": "doc"
        }
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://ny.matrix.ms.com",
            "Referer": "https://ny.matrix.ms.com/eqr/research/ui/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Google Chrome";v = "119", "Chromium";v = "119", "Not?A_Brand";v = "24"',
            "sec-ch-ua-mobile": '?0',
            "sec-ch-ua-platform": '"Windows"',
            "Cookie": f"{cookies}",
            "content-type": "application/json"
        }
        try:
            reSp = requests.request("POST", url, json=payload, headers=headers, timeout=(30.0, 60.0))
            json_data = json.loads(reSp.text)
            documents = json_data['feedServiceResponse']
            dir_name = PATH + "/" + "sub"
            if not os.path.isdir(dir_name):
                os.makedirs(dir_name, mode=0o777, exist_ok=False)
            for docs in documents:
                doc = docs['reports'][0]
                if not doc['pd']:
                    continue
                doc_publish_time = int(time.mktime(time.strptime(doc['pd'][0:19], '%Y-%m-%dT%H:%M:%S'))) * 1000
                if doc_publish_time <= int(time.mktime(time.strptime(newest_time, '%Y-%m-%d %H:%M:%S'))) * 1000:
                    page = 6
                    break
                if 'pdf' not in doc['dt']:
                    continue
                
                url = "https://ny.matrix.ms.com/eqr/article/webapp/services/published/article/frontmatter"
                querystring = {"uuid": f"{doc['id']}"}
                response = requests.request("GET", url, headers=headers, params=querystring)
                response_json = json.loads(response.text)
                pdfRenditionUrl = response_json['frontMatter']['pdfRenditionUrl']
                
                same_res = get_same_source(source=MS + pdfRenditionUrl)
                if len(same_res) > 0:
                    continue
                if pdfRenditionUrl:
                    name = pdfRenditionUrl[pdfRenditionUrl.find("pdf") + 4:pdfRenditionUrl.rfind("pdf") + 3]
                    if '.zip' in name:
                        continue
                    res = downloadPdf(MS + pdfRenditionUrl, dir_name + '/' + name, payload,
                                      headers)
                    if res:
                        add_file_record(type="pdf",
                                        file_path=dir_name + '/' + name,
                                        title=doc['hl'],
                                        source=MS + pdfRenditionUrl,
                                        attribute=escape_string(json.dumps(docs)),
                                        publish_time=datetime.datetime.strptime(doc['pd'][0:19], '%Y-%m-%dT%H:%M:%S'))
            if doc_publish_time < YEAR22_START_TIME_TIMESTRP:
                page = 6
            else:
                page = page + 1
        except Exception as e:
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
            add_fatal_log(
                message=f'ms sub error:{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
            page = 6


if __name__ == '__main__':
    get_ms_report()
