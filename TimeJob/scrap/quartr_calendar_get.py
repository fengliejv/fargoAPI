import json
import os

import requests
from service.ReportService import *
from service.ResearchService import add_research, add_research_attribute
from lib.Common.utils import clean_none

SOURCE = "quartr"
PATH = f"/home/ibagents/files/research/"


def handle(node, headers, date, symbol):
    
    
    
    try:
        
        
        
        
        
        sql="""
        SELECT t1.*, t2.symbol FROM (
            SELECT * 
            FROM TB_Quartr_Calendar 
            WHERE event_id = %s 
            ORDER BY create_time DESC 
            LIMIT 1
        ) t1
        INNER JOIN TB_Script_CompanyNameCode t2 
        ON t1.company_id = t2.company_code 
        WHERE t2.platform = 'quartr'
        """
        old_event = query_dict(sql, (node['eventId'],))
        if len(old_event) > 0:
            slides_check = node['slides'] and not old_event[0]['slides_file_id']
            report_check = node['report'] and not old_event[0]['report_file_id']
            ts_check = node['transcript'] and not old_event[0]['transcript_file_id']
            audio_check = node['audio'] and not old_event[0]['audio_file_id']
            slides_file_id = old_event[0]['slides_file_id']
            report_file_id = old_event[0]['report_file_id']
            transcript_file_id = old_event[0]['transcript_file_id']
            audio_file_id = old_event[0]['audio_file_id']
            
        else:
            slides_check = node['slides']
            report_check = node['report']
            ts_check = node['transcript']
            audio_check = node['audio']
            slides_file_id = ""
            report_file_id = ""
            transcript_file_id = ""
            audio_file_id = ""
        event_id = f"{uuid.uuid1()}"
        if slides_check or report_check or ts_check or audio_check:
            url = f"https://private.quartr.com/api/v2/events/{node['eventId']}"
            response = requests.request("GET", url, headers=headers)
            if response.status_code != 200:
                return
            data = json.loads(response.text)
            headers = {
                "authority": "files.quartr.com",
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "origin": "https://web.quartr.com",
                "referer": "https://web.quartr.com/",
                "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                "sec-ch-ua-mobile": '?0',
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": 'empty',
                "sec-fetch-mode": 'cors',
                "sec-fetch-site": 'same-site',
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            }
            dir_name = PATH + "/" + symbol.replace(".", "").replace(" ", "").replace("&", "")
            if not os.path.isdir(dir_name):
                os.makedirs(dir_name, mode=0o777, exist_ok=False)
            title = ""
            if data['date']:
                publish_time = datetime.datetime.strptime(data['date'], "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                publish_time = datetime.datetime.now()

            if audio_check and data['audioUrl']:
                p_key = f"{uuid.uuid1()}"
                file_name = os.path.basename(data['audioUrl'])
                file_extension = file_name.rsplit('.', 1)[1]
                
                save_path = f"{PATH}{p_key}.{file_extension}"
                download_status = download_file(file_url=data['audioUrl'], local_file_path=save_path, header=headers)
                
                parse_status = "parse_fail"
                if 'rawTranscriptUrl' in data and data['rawTranscriptUrl']:
                    file_name = os.path.basename(data['rawTranscriptUrl'])
                    file_extension = file_name.rsplit('.', 1)[1]
                    save_path = f"{PATH}{p_key}.{file_extension}"
                    parse_status = None
                    if download_file(file_url=data['rawTranscriptUrl'], local_file_path=save_path,
                                     header=headers):
                        parse_status = "parse_ok"
                if download_status:
                    
                    title = f"[Audio] {node['companyName']} {data['title']}"
                    create_time = datetime.datetime.now()
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    data['tripitaka_uuid'] = p_key
                    if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                              value=json.dumps(clean_none(data)),
                                              create_time=create_time):
                        add_research(p_key=p_key,
                                     event_id=event_id,
                                     business_type="audio",
                                     publish_time=publish_time,
                                     source=SOURCE, title=title, file_type=file_extension,
                                     download_status=download_status, create_time=create_time,
                                     source_url=data['audioUrl'], parse_status=parse_status)
                        audio_file_id = p_key
                        transcript_file_id = p_key

            if report_check and data['reportUrl'] and '.pdf' in data['reportUrl']:
                p_key = f"{uuid.uuid1()}"
                file_name = os.path.basename(data['reportUrl'])
                file_extension = file_name.rsplit('.', 1)[1]
                
                save_path = f"{PATH}{p_key}.{file_extension}"
                download_status = download_file(file_url=data['reportUrl'], local_file_path=save_path, header=headers)
                if download_status:
                    name = ""
                    
                    
                    
                    
                    
                    title = f"[Report] {node['companyName']} {data['title']}"
                    create_time = datetime.datetime.now()
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    data['tripitaka_uuid'] = p_key
                    if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                              value=json.dumps(clean_none(data)),
                                              create_time=create_time):
                        add_research(p_key=p_key,
                                     event_id=event_id,
                                     business_type="report",
                                     publish_time=publish_time,
                                     source=SOURCE, title=title, file_type=file_extension,
                                     download_status=download_status, create_time=create_time,
                                     source_url=data['reportUrl'])
                        report_file_id = p_key

            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            

            if slides_check and data['pdfUrl'] and '.pdf' in data['pdfUrl']:
                p_key = f"{uuid.uuid1()}"
                file_name = os.path.basename(data['pdfUrl'])
                file_extension = file_name.rsplit('.', 1)[1]
                
                save_path = f"{PATH}{p_key}.{file_extension}"
                download_status = download_file(file_url=data['pdfUrl'], local_file_path=save_path, header=headers)
                if download_status:
                    create_time = datetime.datetime.now()
                    title = f"[Slides] {node['companyName']} {data['title']}"
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    data['tripitaka_uuid'] = p_key
                    if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                              value=json.dumps(clean_none(data)),
                                              create_time=create_time):
                        slides_file_id = p_key
                        add_research(p_key=p_key,
                                     event_id=event_id,
                                     business_type="slides",
                                     publish_time=publish_time,
                                     source=SOURCE, title=title, file_type=file_extension,
                                     download_status=download_status, create_time=create_time,
                                     source_url=data['pdfUrl'])

        sql = """
        insert into TB_Quartr_Calendar(uuid,attribute,event_id,company_id,event_type,event_title,company_name,
        live_state,edited_transcript_url,transcript_url,country,slides,audio,report,transcript,gics,
        is_estimate_date,expected_report,expected_pdf,expected_audio,expected_transcript,language_iso_code,
        content_dates,went_live_at,event_time,event_date,create_time,update_time,report_file_id,
        slides_file_id,transcript_file_id,audio_file_id) values 
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        execute(sql, (
            f"{event_id}",
            json.dumps(node),
            node['eventId'],
            node['companyId'],
            node['eventType'],
            node['evenTitle'],
            node['companyName'],
            node['liveState'],
            node['editedLiveJsonTranscriptUrl'],
            node['liveJsonTranscriptUrl'],
            node['country'],
            node['slides'],
            node['audio'],
            node['report'],
            node['transcript'],
            node['gics'],
            node['isEstimatedDate'],
            node['expectedContent']['report'],
            node['expectedContent']['pdf'],
            node['expectedContent']['audio'],
            node['expectedContent']['transcript'],
            node['languageIsoCode'],
            json.dumps(node['contentDates']),
            datetime.datetime.strptime(node['wentLiveAt'],
                                       "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                "%Y-%m-%d %H:%M:%S") if node['wentLiveAt'] else None,
            date,
            datetime.datetime.fromisoformat(node['date']).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.datetime.now(),
            datetime.datetime.now(),
            report_file_id,
            slides_file_id,
            transcript_file_id,
            audio_file_id
        ))
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
        return


def download_file(file_url, local_file_path, header):
    try:
        response = requests.request("GET", file_url, headers=header)

        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            return True
        else:
            add_error_log(message=f"file download request fail res code:{response.status_code}")
    except Exception as e:
        add_fatal_log(message=f"file download fail:{str(e)}", e=e)


def get_quartr_calendar():
    sql = "select distinct company_code,symbol from TB_Script_CompanyNameCode where platform='quartr'"
    numbers = set()
    numbers_data = query_dict(sql)
    symbols = {}
    for i in numbers_data:
        numbers.add(i['company_code'])
        symbols[i['company_code']] = i['symbol']
    numbers = list(numbers)
    
    
    
    
    
    
    
    
    
    
    
    now_time = datetime.datetime.now()
    start_time = (now_time - datetime.timedelta(weeks=2)).strftime("%Y-%m-%d")
    end_time = (now_time + datetime.timedelta(weeks=2)).strftime("%Y-%m-%d")
    try:
        url = "https://private.quartr.com/api/v2/events/calendar-dates"
        payload = {
            "startDate": start_time,
            "endDate": end_time,
            "timezone": "Asia/Shanghai",
            "companies": numbers,
            "countries": [],
            "gics": [],
            "types": [],
            "marketCapCategories": []
        }

        
        headers = {'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                   'content-type': 'application/json', 'sec-ch-ua-mobile': '?0',
                   'authorization': 'eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI2VFFPVHpfWjZwM0M5alg5SFVLSmxoOTdLN1d4NG5WUGdwSkxxMFlpVHM4In0.eyJleHAiOjE3MzA5NzE1MzYsImlhdCI6MTcyOTc2MTkzNywiYXV0aF90aW1lIjoxNzI5NzYxOTM2LCJqdGkiOiJlZTA4MWU2MS0yNjEzLTQ5ZDgtYjIzNS1jMDZmYjYzZmY2NjQiLCJpc3MiOiJodHRwczovL2F1dGgucXVhcnRyLmNvbS9yZWFsbXMvcHJvZCIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiIwZjBhZDg1YS1mNTM3LTQ1NWUtYmMyOC0zZWMwOTg5NDA0MzIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ3ZWIiLCJzaWQiOiJkZjQ0OGI0Mi02NTNkLTQzNjItOGM3Yy1lNjZiZGE4NzhhYzYiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbImh0dHBzOi8vcHItKi5xdWFydHIudmVyY2VsLmFwcCIsImh0dHBzOi8vd2ViLWRldi5xdWFydHIuY29tIiwiaHR0cDovL2xvY2FsaG9zdDozMDAwLyoiLCJodHRwczovL3dlYi5xdWFydHIuY29tIiwiaHR0cDovL2xvY2FsaG9zdDozMDAwIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsImRlZmF1bHQtcm9sZXMtcHJvZCIsInVtYV9hdXRob3JpemF0aW9uIiwidXNlciJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGVtYWlsIHVzZXJfZGF0YSBwcm9maWxlIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsIm5hbWUiOiJDaGVpbiBEYSIsImlkIjo1ODY3OTYsInByZWZlcnJlZF91c2VybmFtZSI6ImRhY2hlaW4ueEBnbWFpbC5jb20iLCJnaXZlbl9uYW1lIjoiQ2hlaW4iLCJmYW1pbHlfbmFtZSI6IkRhIiwiZW1haWwiOiJkYWNoZWluLnhAZ21haWwuY29tIn0.E7bMKKrYNr9iCchcAtTHmBqWLaJoijHYemGbj0jkN4DPo9frRwoV4Gl82H5jrJvLEaqW-nhDeeQxG0_wfGMnLIl4QOQkj5AtNq5dIllk-Pu-lBS972BEE0M0nHYie_RKUGXu5QSXtrh21t8Ijq3wjQUHStE4meKzUHEqB0HjQfGbIvYqeAOd-t5jWW7QrefGuxRPl9Oouk93pql-Nv5oC1K61CT28CT0IJFcuh_XFLPmvFdjbwBqrx0T-Dn7tSx5SYLoan9jUO2An6c3RslWK60Mxq_XwzJSvdKdP6FX9JC7KI9zgk4xoUW-xgyqfdUdYPKw1bf91sEESQZAjSKKZA',
                   'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:55.0) Gecko/20100101 Firefox/55.0',
                   'sec-ch-ua-platform': '"Windows"', 'accept': '*/*', 'origin': 'https://web.quartr.com',
                   'sec-fetch-site': 'same-site', 'sec-fetch-mode': 'cors', 'sec-fetch-dest': 'empty',
                   'referer': 'https://web.quartr.com/', 'accept-encoding': 'gzip, deflate, br',
                   'accept-language': 'zh-CN,zh;q=0.9'}
        response = requests.request("POST", url, json=payload, headers=headers)
        if response.status_code != 200:
            return
        data = json.loads(response.text)

        for date in data:
            for node in data[date]:
                try:
                    handle(node=node, headers=headers, date=date, symbol=symbols[f"{node['companyId']}"])
                except Exception as e:
                    print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
                    continue
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
        print(str(e))


if __name__ == '__main__':
    get_quartr_calendar()
