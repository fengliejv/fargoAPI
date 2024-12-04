import datetime
import json
import time
from pip._vendor import requests

from TimeJob.scrap.ubs_cookies_get import ubs_get_cookies
from lib.Common.files import research_download_get
from lib.Common.utils import clean_none
from service.ReportService import *
from service.ResearchService import get_research_newest, add_research, add_research_attribute

SOURCE = 'ubs'
UBS = "https://neo.ubs.com/"
UBS_PDF_DOWNLOAD_URL = f'{UBS}api/super-grid-provider-research/v1/document/'
PATH = f"/home/ibagents/files/research/"


def get_ubs_research():
    cookies = ubs_get_cookies()
    if cookies == "":
        add_error_log(message=f"获取ubs Cookies失败！")
        return
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
    start_str = yesterday.strftime("%Y.%m.%dT%H:%M:%S")
    tomorrow_str = tomorrow.strftime("%Y.%m.%dT%H:%M:%S")

    url = f"https://neo.ubs.com/api/search/v2/research-stream-advanced?q=*&limit=1000"
    payload = {
        "startDateTime": start_str,
        "endDateTime": tomorrow_str
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
        "Cookie": f'arcottest_memory=1732243850678; UBS_NEO_AUTH="UENDOGE5YWMwYjU4YWRlZGYwYTAxOGFkZWY1YWY1ZTAxNDY6PHNhbWw6QXNzZXJ0aW9uIHhtbG5zOnNhbWw9InVybjpvYXNpczpuYW1lczp0YzpTQU1MOjEuMDphc3NlcnRpb24iIEFzc2VydGlvbklEPSJTTTUxNWEwNzU4OWE0MDNiY2M1ZWZmOGY4MjhlMmI4NTU1MjViNDAwOGRiIiBJc3N1ZUluc3RhbnQ9IjIwMjQtMTEtMjJUMDI6NTE6NTQuOTc4WiIgSXNzdWVyPSJJc3N1ZXJOb3RTcGVjaWZpZWRCeVRYTSIgTWFqb3JWZXJzaW9uPSIxIiBNaW5vclZlcnNpb249IjAiPjxzYW1sOkNvbmRpdGlvbnMgTm90QmVmb3JlPSIyMDI0LTExLTIyVDAyOjUxOjU0Ljk3OFoiIE5vdE9uT3JBZnRlcj0iMjAyNC0xMS0yMlQxNDo1MTo1NC45NzhaIi8+PHNhbWw6QXV0aGVudGljYXRpb25TdGF0ZW1lbnQgQXV0aGVudGljYXRpb25JbnN0YW50PSIyMDI0LTExLTIyVDAyOjUxOjU0Ljk3OFoiIEF1dGhlbnRpY2F0aW9uTWV0aG9kPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoxLjA6YW06dW5zcGVjaWZpZWQiPjxzYW1sOlN1YmplY3Q+PHNhbWw6TmFtZUlkZW50aWZpZXIgRm9ybWF0PSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoxLjA6YXNzZXJ0aW9uIiBOYW1lUXVhbGlmaWVyPSIiPk5vIFN1YmplY3QgTmFtZSBQcm92aWRlZDwvc2FtbDpOYW1lSWRlbnRpZmllcj4KPHNhbWw6U3ViamVjdENvbmZpcm1hdGlvbj48c2FtbDpDb25maXJtYXRpb25NZXRob2Q+dXJuOm9hc2lzOm5hbWVzOnRjOlNBTUw6MS4wOmNtOnNlbmRlci12b3VjaGVzPC9zYW1sOkNvbmZpcm1hdGlvbk1ldGhvZD4KPHNhbWw6U3ViamVjdENvbmZpcm1hdGlvbkRhdGE+clV4cUJrbzBCOGNJMzBrd1U2VkR2MU9ibjVJQUlEb2tRQjF6bUpGamtVd0RlMVdMUjhkdXE4ZnF5VVFCTGpnVzFzYjVRS0FTUkpOeVNxOTBVc0d4TmxxbEowZ0hsWjZxd1dNWUkySTQzNnlEbGFIYU1WUk5jcnNhVVd0Z3RaSXl6ZUZQY0N1bGVuNkxCN3RQNGdrajl0bGdzTzBETGR3eWRqZDNVa0NXdEZ5MlhGek5HUXRlSmtTYklwVDh0aEZaSkI2WkQ5dTFQd0lRYW5UYW1Nblk0eGE1bzFPR2x4Z2ZWSkF5bTR6bDFwWDZDbWV2VHR3eWtyNE9ORGZNcGJETC9rWE1yZUp3bTFJanRqSklkcXJHSmpwaGloVXN6b2dXRVkvVkpZcFUxZVZNSkYrWFdsTFF5WHk5ZFRyRWI2Sy90WjdvNWdHdCtqOHNxMDBDM1ArOHVWWUNmWEVoZEtydmpZYVVDdFdvV1lLV0tRNHVHVk92VWJIOEFWMXNhMnExVS9oQVAzdVgzVU0wUDRpYlJ4VG5XUmNBbUdQSkxNaFhPRFBQbHNveFV4dXAzYUIxcklzMFc3K0wrUDZBZy8rVk1YVHdzQy9rWGloTllNbmNmZXpMdGliK0pDb0FaYmtQTTNhQnJEYUp2eXBqL3RxVmFQQzIxdld4a0tUamZJL1J3Ukd3RlRTSFgxbDVHVHgySVdWU3hLM2RTSm9YL1R5RkFDSUx5bHozcEVHaVo2eXBWR0tNK1p2N3VTVlZYSkNyNEtNWHNFREc3c1YxSDc1aUNvdnI0bnh2SmVMdXJTV3VUVzl0TExBVU9TRWhZQmlxc3E2STJocFVINVA3WXNmeEx1MHRnbFJaZnF1Q0grSDBxY3J0U3N0bTlLNWRmV0xqVGFxcHZNTVBhRGoyZmZWOWFOM29LcFE3OWg5TXBaSnNtWWI5Qk9uZGFLcGtoM0RBci8zcXdmK3lINnhFcm0rVVA3c1N1OWRneXZodE5uTkZqb0VWYUgyb2dwRERkaS9MYnM4R2Q3bHkzQk5LODMwQ2oxY01ENGpzdkZ4SHUyVkdmVGZZVVBrSFE4YjdUZG1GV1pBZDZZT3BUckpxV0NyVWpwbVRzYUU1TFJTamRvVzVzVGdrK2c3OHV4eFJmbG5IY1BnT2tWeS9JbDJnN3ZQQnkwU3hMQ0F1WGM5ZGVnem5OR0tpWmhyL2MydW1DTGVqNFNKOEIraW01UTZNVHZxd2wrTGlFVmo4cHkyQXc5enltZTMyRG8reFJnWjByL0F4dTdHV0ZpTmJGdG1KRFc0UnRHNitXVXNQZHFWamNGa1B3ZURQWG02YU11dExtNTlKclhMWGtqUUZKa1dhQzVoTUVLR1FSR01Za2VxWEZid2FyK09qMnc9PTwvc2FtbDpTdWJqZWN0Q29uZmlybWF0aW9uRGF0YT4KPC9zYW1sOlN1YmplY3RDb25maXJtYXRpb24+Cjwvc2FtbDpTdWJqZWN0Pgo8L3NhbWw6QXV0aGVudGljYXRpb25TdGF0ZW1lbnQ+Cjwvc2FtbDpBc3NlcnRpb24+"; UBS_NEO_SESSION=UENDOGE5YWMwYjU4YWRlZGYwYTAxOGFkZWY1YWY1ZTAxNDY6MTczMjI0MzkxNDk3ODoxNzMyMjg3MTE0OTc4OmZhbHNlOnJlbWVtYmVybWU=; UBS_NEO_LOGIN_PAGE=lite; UBS_NEO_USER=UENDOGE5YWMwYjU4YWRlZGYwYTAxOGFkZWY1YWY1ZTAxNDY6SmVmZmVyc29u; UBS_NEO_WEBSHELL_ENTITLED=true; UBS_NEO_APP=default; UBS_NEO_TIMEZONE=0; ECMChatStoreInUse=true; ADRUM=s=1732246569632&r=https%3A%2F%2Fneo.ubs.com%2Fsearch%2Fresearch%3F1909926359; UBS_NEO_RELOADTIME=1732287114978; UBS_NEO_APPS=j%3A%5B%7B%22id%22%3A%22NEO%22%2C%22visited%22%3Atrue%7D%5D; UBS_NEO_WEBSHELL_ENABLED=true; UBS_NEO_SHELL_STICKY="8a7340f53f78266e"'
    }
    try:
        reSp = requests.request("POST", url, json=payload, headers=headers, timeout=(30.0, 60.0))
        json_data = json.loads(reSp.text)
        documents = json_data['categories']['research']['results']
        documents.reverse()
        for doc in documents:
            try:
                if not doc['pubDate']:
                    continue
                publish_time = time.strptime(doc['pubDate'], '%Y-%m-%dT%H:%M:%S.%f')
                doc_publish_time = int(time.mktime(publish_time)) * 1000
                if doc_publish_time <= newest_timestamp:
                    continue
                file_type = doc.get('fileType', None)

                download_status = False
                p_key = f"{uuid.uuid1()}"
                doc['tripitaka_uuid'] = p_key
                source_url = UBS_PDF_DOWNLOAD_URL + doc.get('neoUrlPath').rpartition('/')[-1] if doc.get('neoUrlPath',
                                                                                                         None) else None
                if file_type == 'Acrobat':
                    file_type = 'pdf'
                if file_type == 'Excel':
                    file_type = 'xls'
                if file_type == 'HTM':
                    file_type = 'html'
                if file_type == 'ZIP':
                    file_type = 'zip'
                if source_url and file_type == 'pdf':
                    download_status = research_download_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.pdf",
                                                            data_row=payload,
                                                            header=headers)
                if source_url and file_type == 'zip':
                    download_status = research_download_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.zip",
                                                            data_row=payload,
                                                            header=headers)
                if source_url and file_type == 'xls':
                    download_status = research_download_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.xls",
                                                            data_row=payload,
                                                            header=headers)
                if source_url and file_type == 'html':
                    download_status = research_download_get(file_url=source_url, local_file_path=f"{PATH}{p_key}.html",
                                                            data_row=payload,
                                                            header=headers)
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
    get_ubs_research()
