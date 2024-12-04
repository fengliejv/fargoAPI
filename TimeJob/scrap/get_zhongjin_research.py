import datetime
import json
import re
import time
import uuid

from TimeJob.scrap.cicc_cookies_get import cicc_get_cookies
from lib.Common.files import research_get
from lib.Common.utils import make_request
from service.ReportService import add_fatal_log
from lib.Common.utils import clean_none
from service.ResearchService import add_research, get_research_recently, add_research_attribute

SOURCE = 'cicc'
PATH = f"/home/ibagents/files/research/"


def get_zj_research():
    cookies = cicc_get_cookies()
    if not cookies:
        return
    
    get_from_search(cookies)


def get_from_search(cookies):
    
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=2)
    tomorrow = today + datetime.timedelta(days=1)
    start_str = yesterday.strftime('%Y-%m-%d')
    tomorrow_str = tomorrow.strftime('%Y-%m-%d')
    time_range = f'{start_str}..{tomorrow_str}'
    print(time_range)
    recently_data = get_research_recently(platform=SOURCE, start_time=yesterday)
    titles = []
    for i in recently_data:
        titles.append(i['title'])
    url = f"https://research.cicc.com/frontend/documentsearch/superSearchDocumentList?level=-1&levelChange=-1&startDate={start_str}&endDate={tomorrow_str}&pageNum=1&pageSize=100&keyword=&authorId=&authorName=&stockCode=&stockName=&reportNumCode=&portalCategoryId=&cusPageRange=0,&checkBoxTrue=false"

    querystring = {"level": "-1", "levelChange": "-1", "startDate": start_str, "endDate": tomorrow_str,
                   "pageNum": "1", "pageSize": "100", "keyword": "", "authorId": "", "authorName": "", "stockCode": "",
                   "stockName": "", "reportNumCode": "", "portalCategoryId": "", "cusPageRange": "0,",
                   "checkBoxTrue": "false"}
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://research.cicc.com/searcher/view",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "cookie": cookies
    }
    try:
        reSp = make_request("GET", url, headers=headers, params=querystring)
        json_data = json.loads(reSp.text)
        documents = json_data['data']
        documents.reverse()
        handle_count = 0
        for doc in documents:
            try:
                handle_count += 1
                if not doc['publishTime']:
                    continue
                if doc['title'] in titles:
                    continue
                source_url = None
                download_status = False
                publish_time = datetime.datetime.now()
                if '-' in doc['publishTime']:
                    publish_time = time.strptime(doc['publishTime'], '%Y-%m-%d')
                if ',' in doc['publishTime']:
                    publish_time = time.strptime(doc['publishTime'], '%b %d, %Y')
                if '-' not in doc['publishTime'] and ',' not in doc['publishTime']:
                    publish_time = time.strptime(doc['publishTime'], '%b %d %Y')
                p_key = f"{uuid.uuid1()}"
                doc['tripitaka_uuid'] = p_key
                file_type = 'pdf'
                url = f"https://research.cicc.com/document/detail?id={doc['id']}"
                querystring = {"uuid": f"{doc['id']}"}
                response = make_request("GET", url, headers=headers, params=querystring)
                pattern = rf'/document/downloadPdf/cicc-portal-doc/\d+-\d+/{doc["id"]}'
                matches = re.findall(pattern, response.text)
                for match in matches:
                    print(match)
                    source_url = f"https://research.cicc.com{match}"
                if source_url and file_type == 'pdf':
                    download_status = research_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.pdf",
                                                   header=headers)
                else:
                    continue
                create_time = datetime.datetime.now()
                if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                          value=json.dumps(clean_none(doc)),
                                          create_time=create_time):
                    add_research(p_key=p_key, publish_time=publish_time, source=SOURCE, title=doc['title'],
                                 download_status=download_status, create_time=create_time,
                                 file_type=file_type, source_url=source_url)
            except Exception as e:
                error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
                print(error)
                continue
    except Exception as e:
        error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        print(error)
        add_fatal_log(e=e)


if __name__ == '__main__':
    get_zj_research()
