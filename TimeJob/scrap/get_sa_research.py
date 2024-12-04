import json
import time
from pip._vendor import requests

from service.ReportService import *
from lib.Common.utils import clean_none
from service.ResearchService import add_research, add_research_attribute, get_research_recently

SA = "https://seekingalpha.com"
PATH = f"/home/ibagents/files/research/"


def downloadHtml(file_url, local_file_path, header):
    time.sleep(1)
    try:
        
        response = requests.get(url=file_url, headers=header)
        
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(bytes(response.text, encoding='utf-8'))
            add_info_log(message=f"网页下载成功{file_url}")
            print(f'网页下载成功 {file_url}')
            return True
        else:
            add_error_log(message=f"网页下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        print("下载网页时发生错误:", str(e))
        add_fatal_log(message=f"下载网页时发生错误:{str(e)}")


def get_sa_research():
    url = "https://seekingalpha.com/api/v3/articles"
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=3)
    recently_data = get_research_recently(platform='sa', start_time=yesterday)
    titles = []
    for i in recently_data:
        titles.append(i['title'])
    querystring = {"filter[category]": "latest-articles", "filter[since]": "0", "filter[until]": f"0",
                   "include": "author,primaryTickers,secondaryTickers", "isMounting": "false", "page[size]": "40",
                   "page[number]": "1"}
    headers = {
        "authority": "seekingalpha.com",
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "referer": "https://seekingalpha.com/latest-articles",
        "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Cookie": 'machine_cookie=6891739350306; _pcid=%7B%22browserId%22%3A%22m1a73cw3exuq9akm%22%7D; __pat=-14400000; xbc=%7Bkpex%7DXmodQN0PAJz1WfqmDSQ8jw; _gcl_au=1.1.1752591616.1726804969; _ga=GA1.1.1434211427.1726804969; _fbp=fb.1.1726804969788.18028926320844166; _pxvid=347c125c-7705-11ef-a563-d1d18514549c; hubspotutk=108d35cd71a3116dc9db5683231b345e; _pctx=%7Bu%7DN4IgrgzgpgThIC5QDYAMB2AnMzBGAHIqAA4xQBmAlgB6IggA0IALgJ7FR0BqAGiAL78mkWAGVmAQ2aQ6ZAOaUIzWFAAmjEBErKAkuoQA7MABtj-IA; __tae=1727436779183; __tbc=%7Bkpex%7DVE1VN54vKN1l_HQ5zK3bry9X2pt-0xP7hPmDDqFIIQQdI2V6XLHNnbTngMCegKub; _sasource=; _clck=1fygimo%7C2%7Cfqa%7C0%7C1758; __hssrc=1; pxcts=5bd7af49-91a9-11ef-a12d-a25bf54c7e5f; _igt=61daf47e-472b-4327-9ff0-c28ff72bb85d; google_one_tap=sign_in; sailthru_pageviews=1; session_id=de403757-dbde-4233-84cc-06a34fe67ed0; _uetsid=5a769f5091a911ef9957b98ffec6ef76; _uetvid=5a769fd091a911efbad53ff20e323079; sailthru_visitor=f8706bd0-60fd-4a00-a63f-b485846f9ff6; sailthru_hid=21e2d3ad134c116ad09b3d4c64f2b3b665815987edca62982c095e06e4298feaae616b43bed622fe7f7e7cfc; __hstc=234155329.108d35cd71a3116dc9db5683231b345e.1726804971248.1729734203573.1729736601525.7; __hssc=234155329.1.1729736601525; user_id=61170224; user_nick=riiri; user_devices=1%2C2; user_cookie_key=1w3nz7n; u_voc=; has_paid_subscription=true; ever_pro=1; sapu=12; user_remember_token=b01620106e573e56da7cb5d48adbdf7c3ab5091b; gk_user_access=1*premium.archived*1729736621; gk_user_access_sign=d40068045cad313a8dcf2be13761f790e75b169f; userLocalData_mone_session_lastSession=%7B%22machineCookie%22%3A%226891739350306%22%2C%22machineCookieSessionId%22%3A%226891739350306%261729734200268%22%2C%22sessionStart%22%3A1729734200268%2C%22sessionEnd%22%3A1729738439614%2C%22firstSessionPageKey%22%3A%22df1503b1-fc66-45fd-9416-8eccd64ae3d5%22%2C%22isSessionStart%22%3Afalse%2C%22lastEvent%22%3A%7B%22event_type%22%3A%22mousemove%22%2C%22timestamp%22%3A1729736639614%7D%7D; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Flatest-articles%22%2C%22pageKey%22%3A%2223f64284-052e-4076-81e5-267ce01ae81e%22%7D; _ig=61170224; sailthru_content=b6e65dd404e50fc5910df13b45c183dc2f28c8eb693d38f9b332e225c4a6e13fefa669a9d472fcd923a0a253d629f53b3c5f57e561d9315a8a9a86be8720511e5fcdd5b675fead61ac14b7518e17f855fb524416ba80b74367178ef54be32bd989e68a952abdd8585690952f1484c3b8b6f09daf178879a54eed98528b6242aa105a6c3b33449dbc0fb518a57eae2b83f0251524d6c5bdfecb6f06af6381eace; _clsk=18eb1l0%7C1729736640783%7C18%7C0%7Cl.clarity.ms%2Fcollect; _ga_KGRFF2R2C5=GS1.1.1729736590.6.1.1729736641.9.0.0; _px3=facdd81cbcc1bc288804be57703ff8fa1f7c8d7462b815037653c7b7b56df800:jQr3qSrhw/fkud/HNbMlE044lo1e5JzMeN4cO+4gsCHES0C7wGHl/UmHgtn/4QV9QVFExPJgKi/QJSogduqD6w==:1000:d37AJDoay2gMBsWKWp7OPv3m0/J4WDwZl0xIratFI4fxpn6DZfqguacwsnIoh0/Z4fY8g+G4ZpuGGCVMR2JP86zILjkzeyJbhfMafgv8Gyd15ad6rOV/We/JCXqz8wBopaStLsz/XkASlMSBikzCrJSqhrZaa7L4DPvITAWRcSK5uy/4LCWmOWvx3/g3rndzmjSxPL5Z9mHiVT28Djj69RZ/5lwnd/+Iwk8ZX8vv3kU=; _pxde=b32b241e9bfdb8c122cb838839236fd33852b8cbee2b275dc6661490b302147a:eyJ0aW1lc3RhbXAiOjE3Mjk3MzY2NDIxODIsImZfa2IiOjB9'
    }
    try:
        reSp = requests.request("GET", url, headers=headers, params=querystring)
        json_data = json.loads(reSp.text)
        documents = json_data['data']
        for doc in documents:
            try:
                if doc['attributes']['title'] in titles:
                    continue
                if not doc['attributes']['publishOn']:
                    continue
                p_time = doc['attributes']['publishOn']
                p_time = int(time.mktime(time.strptime(p_time[0:len(p_time) - 6], '%Y-%m-%dT%H:%M:%S'))) * 1000
                if doc['links']['self']:
                    p_key = f"{uuid.uuid1()}"
                    create_time = datetime.datetime.now()
                    source_url = SA + doc['links']['self']
                    download_status = downloadHtml(source_url, f'{PATH}{p_key}.html', headers)
                    url = f"https://seekingalpha.com/api/v3/articles/{doc['id']}"
                    querystring = {
                        "include": "author,primaryTickers,secondaryTickers,otherTags,presentations,presentations.slides,author.authorResearch,author.userBioTags,co_authors,promotedService,sentiments"}
                    article_detail = requests.request("GET", url, headers=headers, params=querystring)
                    article_data = json.loads(article_detail.text)
                    
                    if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                              value=json.dumps(article_data),
                                              create_time=create_time):
                        add_research(p_key=p_key,
                                     publish_time=datetime.datetime.fromtimestamp(
                                         p_time / 1000),
                                     parse_status='parse_ok',
                                     business_type='',
                                     source='sa', title=doc['attributes']['title'],
                                     download_status=download_status, create_time=create_time,
                                     file_type='html', source_url=source_url)
            except Exception as e:
                error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
                print(error)
                add_fatal_log(e=e)
    except Exception as e:
        error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        print(error)
        add_fatal_log(e=e)


def get_sa_history(start_time, end_time):
    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    url = "https://seekingalpha.com/api/v3/articles"
    for i in range(1, 80):
        querystring = {"filter[category]": "latest-articles", "filter[since]": "0", "filter[until]": f"0",
                       "include": "author,primaryTickers,secondaryTickers", "isMounting": "false", "page[size]": "25",
                       "page[number]": f"{i}"}
        headers = {
            "authority": "seekingalpha.com",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "referer": "https://seekingalpha.com/latest-articles",
            "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Cookie": 'machine_cookie=6891739350306; _pcid=%7B%22browserId%22%3A%22m1a73cw3exuq9akm%22%7D; __pat=-14400000; xbc=%7Bkpex%7DXmodQN0PAJz1WfqmDSQ8jw; _gcl_au=1.1.1752591616.1726804969; _ga=GA1.1.1434211427.1726804969; _fbp=fb.1.1726804969788.18028926320844166; _pxvid=347c125c-7705-11ef-a563-d1d18514549c; hubspotutk=108d35cd71a3116dc9db5683231b345e; _pctx=%7Bu%7DN4IgrgzgpgThIC5QDYAMB2AnMzBGAHIqAA4xQBmAlgB6IggA0IALgJ7FR0BqAGiAL78mkWAGVmAQ2aQ6ZAOaUIzWFAAmjEBErKAkuoQA7MABtj-IA; __tae=1727436779183; __tbc=%7Bkpex%7DVE1VN54vKN1l_HQ5zK3bry9X2pt-0xP7hPmDDqFIIQQdI2V6XLHNnbTngMCegKub; _sasource=; _clck=1fygimo%7C2%7Cfqa%7C0%7C1758; __hssrc=1; pxcts=5bd7af49-91a9-11ef-a12d-a25bf54c7e5f; _igt=61daf47e-472b-4327-9ff0-c28ff72bb85d; google_one_tap=sign_in; sailthru_pageviews=1; session_id=de403757-dbde-4233-84cc-06a34fe67ed0; _uetsid=5a769f5091a911ef9957b98ffec6ef76; _uetvid=5a769fd091a911efbad53ff20e323079; sailthru_visitor=f8706bd0-60fd-4a00-a63f-b485846f9ff6; sailthru_hid=21e2d3ad134c116ad09b3d4c64f2b3b665815987edca62982c095e06e4298feaae616b43bed622fe7f7e7cfc; __hstc=234155329.108d35cd71a3116dc9db5683231b345e.1726804971248.1729734203573.1729736601525.7; __hssc=234155329.1.1729736601525; user_id=61170224; user_nick=riiri; user_devices=1%2C2; user_cookie_key=1w3nz7n; u_voc=; has_paid_subscription=true; ever_pro=1; sapu=12; user_remember_token=b01620106e573e56da7cb5d48adbdf7c3ab5091b; gk_user_access=1*premium.archived*1729736621; gk_user_access_sign=d40068045cad313a8dcf2be13761f790e75b169f; userLocalData_mone_session_lastSession=%7B%22machineCookie%22%3A%226891739350306%22%2C%22machineCookieSessionId%22%3A%226891739350306%261729734200268%22%2C%22sessionStart%22%3A1729734200268%2C%22sessionEnd%22%3A1729738439614%2C%22firstSessionPageKey%22%3A%22df1503b1-fc66-45fd-9416-8eccd64ae3d5%22%2C%22isSessionStart%22%3Afalse%2C%22lastEvent%22%3A%7B%22event_type%22%3A%22mousemove%22%2C%22timestamp%22%3A1729736639614%7D%7D; LAST_VISITED_PAGE=%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Flatest-articles%22%2C%22pageKey%22%3A%2223f64284-052e-4076-81e5-267ce01ae81e%22%7D; _ig=61170224; sailthru_content=b6e65dd404e50fc5910df13b45c183dc2f28c8eb693d38f9b332e225c4a6e13fefa669a9d472fcd923a0a253d629f53b3c5f57e561d9315a8a9a86be8720511e5fcdd5b675fead61ac14b7518e17f855fb524416ba80b74367178ef54be32bd989e68a952abdd8585690952f1484c3b8b6f09daf178879a54eed98528b6242aa105a6c3b33449dbc0fb518a57eae2b83f0251524d6c5bdfecb6f06af6381eace; _clsk=18eb1l0%7C1729736640783%7C18%7C0%7Cl.clarity.ms%2Fcollect; _ga_KGRFF2R2C5=GS1.1.1729736590.6.1.1729736641.9.0.0; _px3=facdd81cbcc1bc288804be57703ff8fa1f7c8d7462b815037653c7b7b56df800:jQr3qSrhw/fkud/HNbMlE044lo1e5JzMeN4cO+4gsCHES0C7wGHl/UmHgtn/4QV9QVFExPJgKi/QJSogduqD6w==:1000:d37AJDoay2gMBsWKWp7OPv3m0/J4WDwZl0xIratFI4fxpn6DZfqguacwsnIoh0/Z4fY8g+G4ZpuGGCVMR2JP86zILjkzeyJbhfMafgv8Gyd15ad6rOV/We/JCXqz8wBopaStLsz/XkASlMSBikzCrJSqhrZaa7L4DPvITAWRcSK5uy/4LCWmOWvx3/g3rndzmjSxPL5Z9mHiVT28Djj69RZ/5lwnd/+Iwk8ZX8vv3kU=; _pxde=b32b241e9bfdb8c122cb838839236fd33852b8cbee2b275dc6661490b302147a:eyJ0aW1lc3RhbXAiOjE3Mjk3MzY2NDIxODIsImZfa2IiOjB9'
        }
        try:
            reSp = requests.request("GET", url, headers=headers, params=querystring)
            json_data = json.loads(reSp.text)
            documents = json_data['data']

            for doc in documents:
                try:
                    if not doc['attributes']['publishOn']:
                        continue
                    p_time = doc['attributes']['publishOn']
                    p_time = int(time.mktime(time.strptime(p_time[0:len(p_time) - 6], '%Y-%m-%dT%H:%M:%S'))) * 1000
                    p_timestamp = datetime.datetime.fromtimestamp(p_time / 1000)
                    if p_timestamp < start_time or p_timestamp > end_time:
                        return
                    if doc['links']['self']:
                        p_key = f"{uuid.uuid1()}"
                        create_time = datetime.datetime.now()
                        source_url = SA + doc['links']['self']
                        download_status = downloadHtml(source_url, f'{PATH}{p_key}.html', headers)
                        if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                                  value=json.dumps(clean_none(doc)),
                                                  create_time=create_time):
                            add_research(p_key=p_key,
                                         publish_time=datetime.datetime.fromtimestamp(
                                             p_time / 1000),
                                         parse_status='parse_ok',
                                         business_type='',
                                         source='sa', title=doc['attributes']['title'],
                                         download_status=download_status, create_time=create_time,
                                         file_type='html', source_url=source_url)
                except Exception as e:
                    error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
                    print(error)
                    add_fatal_log(e=e)
        except Exception as e:
            error = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
            print(error)
            add_fatal_log(e=e)


if __name__ == '__main__':
    get_sa_research()
    
