import json
import os
import time
import re
from pip._vendor import requests

from TimeJob.scrap.gs_cookies_get import gs_get_cookies
from service.ReportService import *

SOURCE = "gs"
GS = "https://marquee.gs.com"
PATH = f"/home/ibagents/files/{SOURCE}"
YEAR22_START_TIME = '2022-01-01 00:00:00'
YEAR22_END_TIME = '2023-01-01 00:00:00'
YEAR22_START_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_START_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
YEAR22_END_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_END_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
NOW_TIMESTRP = int(time.time())


def downloadImg(url):
    try:
        response = requests.get(GS + url)
        if response.status_code == 200:
            index = url.rfind("/")
            dir_path = PATH + url[0:index]
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path, mode=0o777, exist_ok=False)
            save_path = PATH + url
            with open(save_path, 'wb') as image_file:
                image_file.write(response.content)
            
        else:
            add_error_log(message=f'图片下载失败 {url}')
    except Exception as e:
        add_fatal_log(message=f"下载图片{url}时发生错误:{str(e)}")


def downloadPdf(file_url, local_file_path, data_row, header):
    try:
        
        response = requests.post(url=file_url, json=data_row, headers=header)
        
        if response.status_code == 200:
            
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            
            print(f'文件下载成功 {file_url}')
            
            return True
        else:
            add_error_log(message=f"文件下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        add_fatal_log(message=f"下载文件时发生错误:{str(e)}")


def downloadHtml(file_url, local_file_path, data_row, header):
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
            
            print(f'网页下载成功 {file_url}')
            return True
        else:
            add_error_log(message=f"网页下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        print("下载网页时发生错误:", str(e))
        add_fatal_log(message=f"下载网页时发生错误:{str(e)}")


def get_gs_report():
    company_list = get_all_company_code()
    print(len(company_list))
    cookies = gs_get_cookies()
    if cookies == "":
        add_error_log(message=f"获取gs Cookies失败！")
        return
    print(f"{SOURCE},time:{datetime.datetime.now()}")
    print(cookies)
    add_info_log(message=f"scrap {SOURCE},time:{datetime.datetime.now()}")
    skip = 0
    count = 0
    for item in company_list:
        count = count + 1
        if count < skip or len(item[1]) == 0:
            continue
        page = 1
        
        doc_publish_time = NOW_TIMESTRP
        print(F"爬取{item[0]},page:{page}")
        
        
        newest_article = get_article_newest_time_by_symbol(
            platform=SOURCE, symbol=item[2])
        if len(newest_article) <= 0:
            newest_time = "2024-01-01 00:00:00"
        else:
            newest_time = newest_article[0]['publish_time'][0:19]
        url = 'https://marquee.gs.com/research/search/reports/advanced-search'
        while page < 10:
            data_rows = {
                "facets": "(id_companies_primary CONTAINS_ALL [\"" + item[1] + "\"])",
                "language": "[\"en\"]",
                "page": page,
                "sort": "time",
                "limitTo": "[\"model\"]",
                "filter": "(disciplines_and_assets EQ ${(\"7fc73956-d6fd-11df-a204-00118563711b\")}$ AND NOT report_types EQ ${(\"6b391588-3653-437c-9cf8-d282ac133cb5\")}$ AND totalPages IN [1,99999])",
                "applyHighlighting": True
            }
            reQ_headers = {
                "authority": "marquee.gs.com",
                "accept": "application/prs.gir-search-service.v2+json;charset=UTF-8",
                "accept-language": "zh-CN,zh;q=0.9",
                "content-type": "application/json;charset=UTF-8",
                "origin": "https://marquee.gs.com",
                "referer": "https://marquee.gs.com/content/research/site/search.html?facets=(id_companies_primary%20CONTAINS_ALL%20%5B%22" +
                           item[
                               1] + "%22%5D)&language=%5B%22en%22%5D&page=2&sort=time&limitTo=%5B%22model%22%5D&filter=(disciplines_and_assets%20EQ%20%24%7B(%227fc73956-d6fd-11df-a204-00118563711b%22)%7D%24%20AND%20NOT%20report_types%20EQ%20%24%7B(%226b391588-3653-437c-9cf8-d282ac133cb5%22)%7D%24%20AND%20totalPages%20IN%20%5B1%2C99999%5D)",
                "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Cookie": f"{cookies}"
            }
            try:
                reSp = requests.post(url=url, json=data_rows, headers=reQ_headers, timeout=(30.0, 60.0))
                json_data = json.loads(reSp.text)
                documents = json_data['documents']
                
                dir_name = PATH + "/" + item[0].replace(".", "").replace(" ", "").replace("&", "")
                if not os.path.isdir(dir_name):
                    os.makedirs(dir_name, mode=0o777, exist_ok=False)
                if len(documents) < 1:
                    break
                for doc in documents:
                    if not doc['publicationDateTime']:
                        continue
                    doc_publish_time = doc['publicationDateTime']
                    if int(doc_publish_time / 1000) <= int(
                            time.mktime(time.strptime(newest_time, '%Y-%m-%d %H:%M:%S'))):
                        page = 10001
                        break
                    if doc['downloadPath']:
                        res = downloadPdf("https://marquee.gs.com" + doc['downloadPath'],
                                          dir_name + '/' + doc['id'] + '.pdf', data_rows, reQ_headers)
                        if res:
                            
                            add_file_record(action=str(doc['actions']), type="pdf",
                                            file_path=dir_name + '/' + doc['id'] + '.pdf', profile=doc['synopsis'],
                                            title=doc['distributionHeadline'],
                                            publish_time=datetime.datetime.fromtimestamp(
                                                doc['publicationDateTime'] / 1000),
                                            attribute=escape_string(json.dumps(doc)),
                                            source="https://marquee.gs.com" + doc['downloadPath'], symbol=item[2])
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    

                page = page + 1
            except Exception as e:
                print(f"下载{SOURCE}公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")
                add_fatal_log(
                    message=f"下载{SOURCE}公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")
                continue


if __name__ == '__main__':
    get_gs_report()
