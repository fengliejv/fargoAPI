import json
import os
import time
import re
from pip._vendor import requests

from TimeJob.scrap.ubs_cookies_get import ubs_get_cookies
from lib.Common.files import downloadPdf, downloadHtml
from service.ReportService import *

SOURCE = 'ubs'
UBS = "https://neo.ubs.com/"
UBS_PDF_DOWNLOAD_URL = f'{UBS}api/super-grid-provider-research/v1/document/'
PATH = f"/home/ibagents/files/{SOURCE}"
YEAR22_START_TIME = '2022-01-01 00:00:00'
YEAR22_END_TIME = '2023-01-01 00:00:00'
YEAR22_START_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_START_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
YEAR22_END_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_END_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
NOW_TIMESTRP = int(time.time())



def get_ubs_report():
    company_list = get_all_company_code(type="ubs")
    print(f"{SOURCE},time:{datetime.datetime.now()}")
    cookies = ubs_get_cookies()
    if cookies == "":
        add_error_log(message=f"获取ubs Cookies失败！")
        return
    add_info_log(message=f"scrap {SOURCE},time:{datetime.datetime.now()}")
    skip = 0
    count = 0
    for item in company_list:
        count = count + 1
        if count < skip or len(item[1]) == 0:
            print(f"skip:{item[0]}")
            continue
        page = 0

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
        while page < 10:
            url = f"https://neo.ubs.com/api/search/v2/research-stream-advanced?q=*&limit=20&offset={page * 20}"
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
                "Cookie": f'arcottest_memory=1732243850678; UBS_NEO_AUTH="UENDOGE5YWMwYjU4YWRlZGYwYTAxOGFkZWY1YWY1ZTAxNDY6PHNhbWw6QXNzZXJ0aW9uIHhtbG5zOnNhbWw9InVybjpvYXNpczpuYW1lczp0YzpTQU1MOjEuMDphc3NlcnRpb24iIEFzc2VydGlvbklEPSJTTTUxNWEwNzU4OWE0MDNiY2M1ZWZmOGY4MjhlMmI4NTU1MjViNDAwOGRiIiBJc3N1ZUluc3RhbnQ9IjIwMjQtMTEtMjJUMDI6NTE6NTQuOTc4WiIgSXNzdWVyPSJJc3N1ZXJOb3RTcGVjaWZpZWRCeVRYTSIgTWFqb3JWZXJzaW9uPSIxIiBNaW5vclZlcnNpb249IjAiPjxzYW1sOkNvbmRpdGlvbnMgTm90QmVmb3JlPSIyMDI0LTExLTIyVDAyOjUxOjU0Ljk3OFoiIE5vdE9uT3JBZnRlcj0iMjAyNC0xMS0yMlQxNDo1MTo1NC45NzhaIi8+PHNhbWw6QXV0aGVudGljYXRpb25TdGF0ZW1lbnQgQXV0aGVudGljYXRpb25JbnN0YW50PSIyMDI0LTExLTIyVDAyOjUxOjU0Ljk3OFoiIEF1dGhlbnRpY2F0aW9uTWV0aG9kPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoxLjA6YW06dW5zcGVjaWZpZWQiPjxzYW1sOlN1YmplY3Q+PHNhbWw6TmFtZUlkZW50aWZpZXIgRm9ybWF0PSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoxLjA6YXNzZXJ0aW9uIiBOYW1lUXVhbGlmaWVyPSIiPk5vIFN1YmplY3QgTmFtZSBQcm92aWRlZDwvc2FtbDpOYW1lSWRlbnRpZmllcj4KPHNhbWw6U3ViamVjdENvbmZpcm1hdGlvbj48c2FtbDpDb25maXJtYXRpb25NZXRob2Q+dXJuOm9hc2lzOm5hbWVzOnRjOlNBTUw6MS4wOmNtOnNlbmRlci12b3VjaGVzPC9zYW1sOkNvbmZpcm1hdGlvbk1ldGhvZD4KPHNhbWw6U3ViamVjdENvbmZpcm1hdGlvbkRhdGE+clV4cUJrbzBCOGNJMzBrd1U2VkR2MU9ibjVJQUlEb2tRQjF6bUpGamtVd0RlMVdMUjhkdXE4ZnF5VVFCTGpnVzFzYjVRS0FTUkpOeVNxOTBVc0d4TmxxbEowZ0hsWjZxd1dNWUkySTQzNnlEbGFIYU1WUk5jcnNhVVd0Z3RaSXl6ZUZQY0N1bGVuNkxCN3RQNGdrajl0bGdzTzBETGR3eWRqZDNVa0NXdEZ5MlhGek5HUXRlSmtTYklwVDh0aEZaSkI2WkQ5dTFQd0lRYW5UYW1Nblk0eGE1bzFPR2x4Z2ZWSkF5bTR6bDFwWDZDbWV2VHR3eWtyNE9ORGZNcGJETC9rWE1yZUp3bTFJanRqSklkcXJHSmpwaGloVXN6b2dXRVkvVkpZcFUxZVZNSkYrWFdsTFF5WHk5ZFRyRWI2Sy90WjdvNWdHdCtqOHNxMDBDM1ArOHVWWUNmWEVoZEtydmpZYVVDdFdvV1lLV0tRNHVHVk92VWJIOEFWMXNhMnExVS9oQVAzdVgzVU0wUDRpYlJ4VG5XUmNBbUdQSkxNaFhPRFBQbHNveFV4dXAzYUIxcklzMFc3K0wrUDZBZy8rVk1YVHdzQy9rWGloTllNbmNmZXpMdGliK0pDb0FaYmtQTTNhQnJEYUp2eXBqL3RxVmFQQzIxdld4a0tUamZJL1J3Ukd3RlRTSFgxbDVHVHgySVdWU3hLM2RTSm9YL1R5RkFDSUx5bHozcEVHaVo2eXBWR0tNK1p2N3VTVlZYSkNyNEtNWHNFREc3c1YxSDc1aUNvdnI0bnh2SmVMdXJTV3VUVzl0TExBVU9TRWhZQmlxc3E2STJocFVINVA3WXNmeEx1MHRnbFJaZnF1Q0grSDBxY3J0U3N0bTlLNWRmV0xqVGFxcHZNTVBhRGoyZmZWOWFOM29LcFE3OWg5TXBaSnNtWWI5Qk9uZGFLcGtoM0RBci8zcXdmK3lINnhFcm0rVVA3c1N1OWRneXZodE5uTkZqb0VWYUgyb2dwRERkaS9MYnM4R2Q3bHkzQk5LODMwQ2oxY01ENGpzdkZ4SHUyVkdmVGZZVVBrSFE4YjdUZG1GV1pBZDZZT3BUckpxV0NyVWpwbVRzYUU1TFJTamRvVzVzVGdrK2c3OHV4eFJmbG5IY1BnT2tWeS9JbDJnN3ZQQnkwU3hMQ0F1WGM5ZGVnem5OR0tpWmhyL2MydW1DTGVqNFNKOEIraW01UTZNVHZxd2wrTGlFVmo4cHkyQXc5enltZTMyRG8reFJnWjByL0F4dTdHV0ZpTmJGdG1KRFc0UnRHNitXVXNQZHFWamNGa1B3ZURQWG02YU11dExtNTlKclhMWGtqUUZKa1dhQzVoTUVLR1FSR01Za2VxWEZid2FyK09qMnc9PTwvc2FtbDpTdWJqZWN0Q29uZmlybWF0aW9uRGF0YT4KPC9zYW1sOlN1YmplY3RDb25maXJtYXRpb24+Cjwvc2FtbDpTdWJqZWN0Pgo8L3NhbWw6QXV0aGVudGljYXRpb25TdGF0ZW1lbnQ+Cjwvc2FtbDpBc3NlcnRpb24+"; UBS_NEO_SESSION=UENDOGE5YWMwYjU4YWRlZGYwYTAxOGFkZWY1YWY1ZTAxNDY6MTczMjI0MzkxNDk3ODoxNzMyMjg3MTE0OTc4OmZhbHNlOnJlbWVtYmVybWU=; UBS_NEO_LOGIN_PAGE=lite; UBS_NEO_USER=UENDOGE5YWMwYjU4YWRlZGYwYTAxOGFkZWY1YWY1ZTAxNDY6SmVmZmVyc29u; UBS_NEO_WEBSHELL_ENTITLED=true; UBS_NEO_APP=default; UBS_NEO_TIMEZONE=0; ECMChatStoreInUse=true; ADRUM=s=1732246569632&r=https%3A%2F%2Fneo.ubs.com%2Fsearch%2Fresearch%3F1909926359; UBS_NEO_RELOADTIME=1732287114978; UBS_NEO_APPS=j%3A%5B%7B%22id%22%3A%22NEO%22%2C%22visited%22%3Atrue%7D%5D; UBS_NEO_WEBSHELL_ENABLED=true; UBS_NEO_SHELL_STICKY="8a7340f53f78266e"'
            }
            try:
                reSp = requests.request("POST", url, json=payload, headers=headers, timeout=(30.0, 60.0))
                json_data = json.loads(reSp.text)
                documents = json_data['categories']['research']['results']
                dir_name = PATH + "/" + item[0].replace(".", "").replace(" ", "").replace("&", "")
                if not os.path.isdir(dir_name):
                    os.makedirs(dir_name, mode=0o777, exist_ok=False)
                for doc in documents:
                    if not doc['pubDate']:
                        continue
                    doc_publish_time = int(time.mktime(time.strptime(doc['pubDate'], '%Y-%m-%dT%H:%M:%S.%f'))) * 1000
                    if doc_publish_time <= int(time.mktime(time.strptime(newest_time, '%Y-%m-%d %H:%M:%S'))) * 1000:
                        page = 11
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
                                            publish_time=datetime.datetime.strptime(doc['pubDate'],
                                                                                    '%Y-%m-%dT%H:%M:%S.%f'),
                                            symbol=item[2])



                if doc_publish_time < YEAR22_START_TIME_TIMESTRP:
                    page = 11
                else:
                    page = page + 1
            except Exception as e:
                print(f"下载公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")
                add_fatal_log(
                    message=f"下载公司报告异常,公司名：{item[0]},公司code:{item[1]},报告页码：{page},报错:{str(e)}")
                page = 11


if __name__ == '__main__':
    get_ubs_report()
