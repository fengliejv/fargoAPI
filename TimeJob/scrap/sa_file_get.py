import datetime
import io
import json
import os
import time
import re

from bs4 import BeautifulSoup
import requests
from pymysql.converters import escape_string

from service.ReportService import add_info_log, get_article_newest_time_by_company, add_error_log, add_fatal_log, \
    add_file_record

SA = "https://seekingalpha.com"
PATH = "/home/ibagents/files/sa"
YEAR22_START_TIME = '2022-01-01 00:00:00'
YEAR22_END_TIME = '2023-01-01 00:00:00'
YEAR22_START_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_START_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
YEAR22_END_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_END_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
NOW_TIMESTRP = int(time.time())
CLIENT_ID = 'cioinsight-backend'
CLIENT_SECRET = '0258d90f-fa98-4b16-916c-51c8a38c3a46'
ARTICLE_TABLE = 'native-table-d0be72f8-715d-4bd4-975d-20691d09d2ad'
FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'


def downloadHtml(file_url, local_file_path, header):
    time.sleep(1)
    try:
        
        response = requests.get(url=file_url, headers=header, timeout=300)
        
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(bytes(response.text, encoding='utf-8'))
            
            print(f'网页下载成功 {file_url}')
            return True
        else:
            add_error_log(message=f"网页下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        print("下载网页时发生错误:", str(e))
        add_fatal_log(message=f"下载网页时发生错误:{str(e)}")


def get_sa_report():
    file = open("/home/ibagents/bugs/static/sacompany.txt", "r")  
    content = file.readlines()  
    company_list = []
    for line in content:  
        line = line.strip('\n')
        company_list.append(line)
    doc_dict = []
    
    file = open("/home/ibagents/bugs/static/stock-company.csv", "r")  
    content = file.readlines()  
    com_dict = dict()
    for line in content:  
        line = line.strip('\n')
        temp = line.split(",")
        if temp[1]:
            com_dict.setdefault(temp[1], temp[0])
    print(f"sa,time:{datetime.datetime.now()}")
    add_info_log(message=f"scrap sa,time:{datetime.datetime.now()}")
    skip = 0
    count = 0
    for item in company_list:
        time.sleep(1)
        count = count + 1
        if count < skip:
            continue
        page = 1
        while page < 10:
            time.sleep(1)
            
            doc_publish_time = NOW_TIMESTRP
            print(F"爬取{item},page:{page}")
            
            
            newest_artcile = get_article_newest_time_by_company(platform='sa', company_code=item)
            if len(newest_artcile) <= 0:
                break
            newest_time = newest_artcile[0]['publish_time'][0:19]
            url = f"https://seekingalpha.com/api/v3/symbols/{item}/analysis?filter[{int(YEAR22_START_TIME_TIMESTRP / 1000)}]=0&filter[until]={int(datetime.datetime.now().timestamp())}&id=jd&include=author%2CprimaryTickers%2CsecondaryTickers%2Csentiments&isMounting=false&page[size]=100&page[number]={page}"
            reQ_headers = {
                "authority": "seekingalpha.com",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "max-age=0",
                "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Cookie": "_sasource=; session_id=87ed7622-049e-4629-ba4a-bf99f6716256; machine_cookie=3354189653688; _gcl_au=1.1.1121556927.1702975870; _ga=GA1.1.1875601687.1702975870; _pcid=%7B%22browserId%22%3A%22lqc3v39ph8qhi5yc%22%7D; _pcus=eyJ1c2VyU2VnbWVudHMiOm51bGx9; __pat=-18000000; _pxvid=c24b73cd-9e4b-11ee-8161-4187c1a4223b; _fbp=fb.1.1702975871988.1893017202; hubspotutk=51f9982809d30fe92c3c3943464f3368; _hjSessionUser_65666=eyJpZCI6IjhmZWJjMTY2LTU4ZWItNTIyMC04Mjk5LWY2ZmRlYTg2YmFlNSIsImNyZWF0ZWQiOjE3MDI5NzU4NzMwODgsImV4aXN0aW5nIjp0cnVlfQ==; __stripe_mid=4972b644-3208-4d83-a7fa-ea65f58fe53d1c8f05; __tac=; __tae=1703384491216; has_paid_subscription=true; ever_pro=1; sailthru_hid=d2cc164bed8f4d11d6fef517bc34f40c6577e303ee179836ae010fccf88df6863b0f7b5d3f6341bcb88cbec8; ubvt=v2%7Ce106ec50-c0f5-4de9-98dd-2201ff4887e0%7C97b939d8-bbe2-4ce8-b3c0-aeeb6d4a93d2%3Aa%3Asingle%7Ccad24e48-e47a-409d-88aa-bf8105fb2be5%3Ao%3Aweighted; _hjIncludedInSessionSample_65666=0; _hjSession_65666=eyJpZCI6ImZiNjFiYzczLWY3MWQtNDZhMy1hNGNlLTU0ZTI2OTNlM2U4YSIsImMiOjE3MDM0NjY0NTYxODksInMiOjAsInIiOjAsInNiIjoxfQ==; _hjAbsoluteSessionInProgress=0; _igt=7d90971f-f74e-4cb7-96c2-31c3b4157c7d; _hjHasCachedUserAttributes=true; __hstc=234155329.51f9982809d30fe92c3c3943464f3368.1702975900221.1703424047620.1703466458560.7; __hssrc=1; _clck=lbxz%7C2%7Cfhu%7C0%7C1453; pxcts=feea3571-a2c1-11ee-aaa9-e6b02d42215b; session_id=87ed7622-049e-4629-ba4a-bf99f6716256; _sasource=; sailthru_pageviews=5; sailthru_content=f0251524d6c5bdfecb6f06af6381eacee1866f68e1fcfa29ce6bc7ad346851cd76e167ae532478b3931cbc3813d1e9dbb410012e7775cd4a945c76456bdadbfac0429ffe151a1ddbfaacea19980a6a5b9881a1366e4f8aa903a9ecc2bb046d88cee9f6e97844b2d50c1a0176d4b403dc5cc4466f5dee8afceba01025771815385861980a7cfec657d15ae640b646bbfc62ea0743f1937e9137cbf9919422957d393d479e9552a5dc8ddc7c93b36a2c2b103d56af557a36fa91a4015898fe78d169ae5b4899d02ce61d4d237c329743c73c5f57e561d9315a8a9a86be8720511e; sailthru_visitor=4d020b7b-7e5e-415d-830d-1f38091c34ec; _uetsid=25c1ebd0a20311ee842523472917f6c9; _uetvid=c1e587d09e4b11eeaabb4b4b30a22be7; __hssc=234155329.5.1703466458560; _ig=59668964; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Fsymbol%2FJD%2Fanalysis%22%2C%22pageKey%22%3A%22f7199c6d-dafc-465d-b2f5-c09f96d70885%22%7D; _clsk=1sw5upg%7C1703466838083%7C4%7C0%7Cl.clarity.ms%2Fcollect; _px=kP3tq2PNZHp9MSs46xo3gT5uoLOQZl8oRh0q2dXkNcGNnAAaSNGolKAfDct/geSof9djQvQkYLE1AVDga3KbYA==:1000:jg8bOO6owKHyCggDGgyAx2ZtgM7OHCnO40yKwoAYIK3giYY+7Dxj22nZSSL2F9laNl+yeZn8Iae2EyzeiFRFi5bJuciXfoJ8iieJ9h01oD8dc58Nzq1c86HUp40sjqVQAtvL7sO+Y2hoB3AVJzxez7JXZHns/eR2i87JFOOO6uRvAKpN8LgMa20LIp1oyB6Evp+Mev8kZcpjDcXL366w36LVi3b3bY8XqwyB35G2KywHcIqO8OgOWluN4XKRyySW7XHvWSEHkr7TXWpEGs5Gog==; _px2=eyJ1IjoiZmVmNWU2MjAtYTJjMS0xMWVlLWIzM2EtNTUwOTFmNGVhNTIyIiwidiI6ImMyNGI3M2NkLTllNGItMTFlZS04MTYxLTQxODdjMWE0MjIzYiIsInQiOjE3MDM0Njc3MzkzMTMsImgiOiI1ZmM4MmRiMjA0YTgzYzAwNGQ4MDg0MGMwZDA1ODIyODg4MjE5ZmUxYjcwNmMxM2I0ZWE3NjIzNTdiMjliNDBiIn0=; _pxde=b5f5f8bc3e7b001ab3907f0873d5b10b6ba7909d79d2c4b8259895d97c382dd8:eyJ0aW1lc3RhbXAiOjE3MDM0NjcyMzkzMTMsImZfa2IiOjB9; xbc=%7Bkpex%7DEo9NCpchNUL4u4vh_rkInPnSp69gvqSXT6vBBf3xZqr7W5KNXQiQMi3AGsp3Vv05E0tZCMK9uUP34FiDfoDL9IqoYms4rBjvaqMsewhmQtTv6x0a54RioaXzG9XeMsToE5YnJLfIBLXpUSrpxU4TuA2VYrOR3b-A004DSXi7Lg4yFF-g6sFwgtqkjyevVTnB4gctrMUsTQ4JrlJeJWcy5voEzikD9VFJG5lShSaNcDUEOeW4yrKrMVc0ma_gnloqshaV4YjQHh5vrxUQ2awCjRLq83JMkjEmpBv4wG-HuU_NhKBe3gN09goXo6So3ElAh4xAONmHdL8wjQOu8_BYKD8bUt-ecF89dwMQ98fqlSQ2JwIAqLtV_ifbGnIzcjCU; user_id=59707562; user_nick=Di+KI3; user_devices=1%2C2; user_cookie_key=1e1wfj0; u_voc=; sapu=212; user_remember_token=b38dca93f287d80d3f6fdc88d73ce0462c9b65de; gk_user_access=1*embargoed.premium.archived*1703467349; gk_user_access_sign=1cc14119eed7a3aa6433065f603c3b7aa140b736; _ga_KGRFF2R2C5=GS1.1.1703466455.7.1.1703467350.45.0.0; __pvi=eyJpZCI6InYtMjAyMy0xMi0yNS0wOS0wNy0zNS03MDItalVBZTV1N1RpeGhHbDZrai00ODM3YzNkMmNjZGY0NGJjMWVhMjk0MDFjYzgzZTZmZSIsImRvbWFpbiI6Ii5zZWVraW5nYWxwaGEuY29tIiwidGltZSI6MTcwMzQ2NzM1MTMwM30%3D; __tbc=%7Bkpex%7DiNfFGIQghoMafy0fBnn9h-RMaGLVRpTXYBFYXe8L1raC4mEPWPxUjROLowQv0MrY; _pctx=%7Bu%7DN4IgrgzgpgThIC5QFYCcB2ADO5A2ATIqAA4xQBmAlgB6IggA0IALgJ7FR0BqAGiAL78mkWAGVmAQ2aQ6ZAOaUIzWFAAmjEBErKAkuoQA7MABtj-IA"}
            try:

                reSp = requests.get(url=url, headers=reQ_headers)
                json_data = json.loads(reSp.text)
                documents = json_data['data']
                
                dir_name = PATH + "/" + item.replace(".", "").replace(" ", "").replace("&", "")
                if not os.path.isdir(dir_name):
                    os.makedirs(dir_name, mode=0o777, exist_ok=False)

                for doc in documents:
                    if not doc['attributes']['publishOn']:
                        continue
                    p_time = doc['attributes']['publishOn']
                    p_time = int(time.mktime(time.strptime(p_time[0:len(p_time) - 6], '%Y-%m-%dT%H:%M:%S'))) * 1000
                    if p_time <= int(time.mktime(time.strptime(newest_time, '%Y-%m-%d %H:%M:%S'))) * 1000:
                        page = 10001
                        break

                    if doc['links']['self']:
                        
                        
                        
                        time.sleep(1)
                        res = downloadHtml(SA + doc['links']['self'], dir_name + '/' + doc['id'] + '.html',
                                           reQ_headers)
                        if res:
                            file_path = dir_name + '/' + doc['id'] + '.html'
                            publish_time = datetime.datetime.fromtimestamp(
                                p_time / 1000)
                            add_res = add_file_record(action="", type="html",
                                                      file_path=file_path, profile="",
                                                      title=doc['attributes']['title'],
                                                      source=SA + doc['links']['self'],
                                                      publish_time=publish_time,
                                                      attribute=escape_string(json.dumps(doc)), symbol=item[2])
                page = page + 1
            except Exception as e:
                print(f"定时更新公司报告异常,公司名：{item},报告页码：{page},报错:{str(e)}")
                add_fatal_log(message=f"定时更新公司报告异常,公司名：{item},报告页码：{page},报错:{str(e)}")
                break


if __name__ == '__main__':
    get_sa_report()
