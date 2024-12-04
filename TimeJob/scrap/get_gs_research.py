import datetime
import json

from pip._vendor import requests

from TimeJob.scrap.gs_cookies_get import gs_get_cookies
from lib.Common.files import research_download
from lib.Common.utils import clean_none
from service.ReportService import *
from service.ResearchService import get_research_newest, add_research, add_research_attribute

SOURCE = "gs"
GS = "https://marquee.gs.com"
PATH = f"/home/ibagents/files/research/"


def get_gs_research():
    cookies = gs_get_cookies()
    if cookies == "":
        add_error_log(message=f"获取gs Cookies失败！")
        return
    newest_article = get_research_newest(platform=SOURCE)
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    if len(newest_article) <= 0:
        newest_time = yesterday
    else:
        newest_time = newest_article[0]['publish_time']
    print(newest_time)
    today_str = today.strftime('%Y-%m-%dT%H:%M:%S')
    yesterday_str = yesterday.strftime('%Y-%m-%dT%H:%M:%S')
    newest_timestamp = int(newest_time.timestamp()) * 1000
    url = 'https://marquee.gs.com/research/search/reports/advanced-search'
    data_rows = {
        "facets": "()",
        "language": "[\"en\"]",
        "page": 2,
        "sort": "time",
        "limitTo": "[\"model\"]",
        "filter": "(sources EQ ${(\"e3c3ad77-fd99-4f30-8614-66e8870877c9\")}$ OR disciplines_and_assets EQ ${(Research)}$ AND totalPages IN [1,99999] AND publicationDateTime IN [" + f'{yesterday_str},{today_str}' + "])",
        "applyHighlighting": True
    }
    reQ_headers = {
        "authority": "marquee.gs.com",
        "accept": "application/prs.gir-search-service.v2+json;charset=UTF-8",
        "accept-language": "zh-CN,zh;q=0.9",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://marquee.gs.com",
        "referer": "https://marquee.gs.com/content/research/site/search.html?facets=()&language=%5B%22en%22%5D&page=1&sort=time&limitTo=%5B%22model%22%5D&filter=(sources%20EQ%20%24%7B(%22e3c3ad77-fd99-4f30-8614-66e8870877c9%22)%7D%24%20OR%20disciplines_and_assets%20EQ%20%24%7B(Research)%7D%24%20AND%20totalPages%20IN%20%5B1%2C99999%5D%20AND%20publicationDateTime%20IN%20%5B2024-07-15T16%3A00%3A00%2C2024-07-17T16%3A00%3A00%5D)",
        "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Cookie": f"{cookies}"
    }
    result = []
    go_on = True
    for i in range(1, 20):
        data_rows['page'] = i
        try:
            if not go_on:
                break
            reSp = requests.post(url=url, json=data_rows, headers=reQ_headers, timeout=(30.0, 60.0))
            json_data = json.loads(reSp.text)
            documents = json_data['documents']
            for doc in documents:
                if not doc['publicationDateTime']:
                    continue
                doc_publish_time = doc['publicationDateTime']
                if doc_publish_time <= newest_timestamp:
                    go_on = False
                    break
                result.append(doc)
        except Exception as e:
            error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
            print(error)
            add_fatal_log(e=e)
    result.reverse()
    for i in result:
        try:
            file_type = None
            source_url = None
            download_status = False
            p_key = f"{uuid.uuid1()}"
            i['tripitaka_uuid'] = p_key
            if i['downloadPath']:
                file_type = i['downloadPath'].rpartition('.')[-1]
                source_url = "https://marquee.gs.com" + i['downloadPath']
            if source_url and file_type == 'pdf':
                download_status = research_download(file_url=source_url, local_file_path=f"{PATH}{p_key}.pdf",
                                                    data_row=data_rows,
                                                    header=reQ_headers)
            if source_url and file_type == 'zip':
                download_status = research_download(file_url=source_url, local_file_path=f"{PATH}{p_key}.zip",
                                                    data_row=data_rows,
                                                    header=reQ_headers)
            if source_url and file_type == 'xls':
                download_status = research_download(file_url=source_url, local_file_path=f"{PATH}{p_key}.xls",
                                                    data_row=data_rows,
                                                    header=reQ_headers)
            create_time = datetime.datetime.now()
            if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                      value=json.dumps(clean_none(i)),
                                      create_time=create_time):
                add_research(p_key=p_key, publish_time=datetime.datetime.fromtimestamp(i['publicationDateTime'] / 1000),
                             source=SOURCE, title=i['distributionHeadline'], file_type=file_type,
                             download_status=download_status, create_time=create_time, source_url=source_url)

        except Exception as e:
            error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
            print(error)
            add_fatal_log(e=e)


if __name__ == '__main__':
    get_gs_research()
