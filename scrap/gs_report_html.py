import json
import os
import time
import re
from pip._vendor import requests

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
        
        response = requests.post(url=file_url, json=data_row, headers=header)
        
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
            add_info_log(message=f"网页下载成功{file_url}")
            print(f'网页下载成功 {file_url}')
            return True
        else:
            add_error_log(message=f"网页下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        print("下载网页时发生错误:", str(e))
        add_fatal_log(message=f"下载网页时发生错误:{str(e)}")


def get_gs_html():
    company_list = get_all_company_code()
    print(company_list)
    skip = 0
    count = 0
    for item in company_list:
        count = count + 1
        if count < skip:
            continue
        page = 1
        while page < 10000:
            
            doc_publish_time = NOW_TIMESTRP
            print(F"爬取{item[0]},page:{page}")
            add_info_log(message=f"爬取{item[0]},page:{page}")
            url = 'https://marquee.gs.com/research/search/reports/advanced-search'
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
                "Cookie": "s_fid=608098FACBF27C56-08ADC58255B359E1; gsFontSize=font--medium; _ga=GA1.2.247162916.1702526428; _ga_NFFERC2KBY=GS1.1.1702534917.1.1.1702534930.0.0.0; _sp_id.7be8=d6a2e938-ade6-4c98-9427-a08c822f106a.1702535961.7.1702971313.1702958611.ea4fb58b-2540-46e0-8ee6-4701d095ee8f; akacd_AWS_origin=3882923703~rv=17~id=a36632549d59171db89544d7d1d7d799; MarqueeLogin=00134HDcAa8EduiuEgvMjGCRRtP9; at_check=true; mboxEdgeCluster=38; mbox=PC
            }
            try:
                reSp = requests.post(url=url, json=data_rows, headers=reQ_headers)
                json_data = json.loads(reSp.text)
                documents = json_data['documents']
                
                dir_name = PATH + "/" + item[0].replace(".", "").replace(" ", "").replace("&", "")
                if not os.path.isdir(dir_name):
                    os.makedirs(dir_name)
                for doc in documents:
                    if not doc['publicationDateTime']:
                        continue
                    doc_publish_time = doc['publicationDateTime']
                    if doc['publicationDateTime'] < YEAR22_START_TIME_TIMESTRP:
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
                    if doc['path']:
                        res = downloadHtml(GS + doc['path'], dir_name + '/' + doc['id'] + '.html', data_rows,
                                           reQ_headers)
                        if res:
                            add_file_record(action=str(doc['actions']), type="html",
                                            file_path=dir_name + '/' + doc['id'] + '.html', profile=doc['synopsis'],
                                            title=doc['distributionHeadline'],
                                            source="https://marquee.gs.com" + doc['path'],
                                            publish_time=datetime.datetime.fromtimestamp(
                                                doc['publicationDateTime'] / 1000),
                                            attribute=escape_string(json.dumps(doc)), symbol=item[2])

                if doc_publish_time < YEAR22_START_TIME_TIMESTRP:
                    page = 10001
                else:
                    page = page + 1
            except Exception as e:
                print(f"下载{SOURCE}公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")
                add_fatal_log(
                    message=f"下载{SOURCE}公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")


if __name__ == '__main__':
    get_gs_html()
