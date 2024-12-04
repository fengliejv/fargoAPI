import datetime
import json
import time
import uuid

from pip._vendor import requests

from TimeJob.scrap.ms_cookies_get import ms_get_cookies
from lib.Common.files import research_download_get
from lib.Common.utils import clean_none
from service.ReportService import add_fatal_log, add_error_log
from service.ResearchService import get_research_newest, add_research, add_research_attribute

SOURCE = 'ms'
MS = "https://ny.matrix.ms.com"
PATH = f"/home/ibagents/files/research/"


def get_ms_research():
    cookies = ms_get_cookies()
    if cookies == "":
        add_error_log(message=f"获取ms Cookies失败！")
        return
    get_from_search(cookies)


def get_from_search(cookies):
    newest_research = get_research_newest(platform=SOURCE)
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    if len(newest_research) <= 0:
        newest_time = yesterday
    else:
        newest_time = newest_research[0]['publish_time']
    print(newest_time)
    newest_timestamp = int(newest_time.timestamp()) * 1000
    tomorrow = today + datetime.timedelta(days=1)
    start_str = yesterday.strftime('%d/%m/%Y')
    tomorrow_str = tomorrow.strftime('%d/%m/%Y')
    time_range = f'{start_str}..{tomorrow_str}'
    print(time_range)
    url = f"https://ny.matrix.ms.com/eqr/research/webapp/rlservices/search/v4/composite.json"
    payload = {
        "compositeRequest": {
            "search": f"date==1D={time_range}",
            "sort": "d",
            "noSearch": False,
            "gn": False,
            "didyoumean": False,
            "lang": "en,zh",
            "prefLang": "zh",
            "countMode": "best",
            "showcard": False,
            "queryID": "bd3b7cca-0786-4e1f-888d-7e3fc287344b",
            "userJourneyId": "9a064dd1-ef5e-49d5-ac73-6a00d758d521",
            "size": 400,
            "page": 1
        },
        "arRequest": {
            "skipSpellCheck": False,
            "invokeAskResearch": False,
            "dateFilter": "",
            "filtersMap": {
                "queryWithoutStopwords": ""
            }
        }
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
        documents = json_data['searchAndCardResponse']['rcsSearchResponse']
        if 'docs' in documents:
            documents = documents['docs']
        else:
            return
        documents.reverse()
        for doc in documents:
            time.sleep(2)
            try:
                if not doc['pd']:
                    continue
                publish_time = time.strptime(doc['pd'][0:19], '%Y-%m-%dT%H:%M:%S')
                doc_publish_time = int(time.mktime(publish_time)) * 1000
                if doc_publish_time <= newest_timestamp:
                    continue
                file_type = None
                p_key = f"{uuid.uuid1()}"
                doc['tripitaka_uuid'] = p_key
                if doc['dt']:
                    file_type = doc['dt'].rpartition('/')[-1]
                source_url = None
                download_status = False
                if file_type == 'xls':
                    source_url = MS + f"/eqr/article/webapp/{doc['id']}?ch=rpext&sch=sr&sr=1"
                    download_status = research_download_get(file_url=source_url,
                                                            local_file_path=f"{PATH}{p_key}.xls",
                                                            data_row=payload,
                                                            header=headers)
                if file_type == 'pdf':
                    url = "https://ny.matrix.ms.com/eqr/article/webapp/services/published/article/frontmatter"
                    querystring = {"uuid": f"{doc['id']}"}
                    response = requests.request("GET", url, headers=headers, params=querystring)
                    response_json = json.loads(response.text)
                    pdfRenditionUrl = response_json['frontMatter']['pdfRenditionUrl']

                    if pdfRenditionUrl:
                        source_url = MS + pdfRenditionUrl
                    if pdfRenditionUrl and 'pdf' in doc['dt']:
                        name = pdfRenditionUrl[pdfRenditionUrl.find("pdf") + 4:pdfRenditionUrl.rfind("pdf") + 3]
                        if '.zip' not in name:
                            time.sleep(5)
                            download_status = research_download_get(file_url=MS + pdfRenditionUrl,
                                                                    local_file_path=f"{PATH}{p_key}.pdf",
                                                                    data_row=payload,
                                                                    header=headers)
                create_time = datetime.datetime.now()
                if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                          value=json.dumps(clean_none(doc)), create_time=create_time, lang=doc['lang']):
                    add_research(p_key=p_key, publish_time=publish_time, source=SOURCE, title=doc['hl'],
                                 download_status=download_status, create_time=create_time,
                                 file_type=file_type, lang=doc['lang'],
                                 source_url=source_url)

            except Exception as e:
                error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
                print(error)
                continue
    except Exception as e:
        error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        print(error)
        add_fatal_log(e=e)


if __name__ == '__main__':
    get_ms_research()
