
import base64
import csv
import datetime
import json
import os.path
import shutil
import time
import uuid

import html2text
import requests
from bs4 import BeautifulSoup
from minio import S3Error
from qcloud_cos import CosConfig, CosS3Client

from TimeJob.scrap.get_fargo_research import get_fargo_research_time
from api.v1.lib.common.mysqlsingle import execute, query_dict
from api.v1.service.FileBasicService import set_file_basic_attr
from api.v1.service.WechatBotService import add_wechat_user
from api.v1.service.WechatService import get_wechat_user
from lib.Common.my_minio import Bucket
from service.ReportService import add_error_log
from service.ResearchService import add_research, add_research_attribute


def add_new_sub(user_id, symbol):
    sql = f"delete from TB_User_Sub where user_id=%s and symbol=%s"
    execute(sql, (user_id, symbol))
    sql = f"insert into TB_User_Sub(uuid,user_id,symbol,update_time) values (%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", user_id, symbol, datetime.datetime.now()))


def get_all_symbol():
    sql = f'select DISTINCT symbol from TB_Script_CompanyNameCode'
    data = query_dict(sql)
    return data


def sub_all_symbol():
    data = get_all_symbol()
    for i in data:
        count = add_new_sub("02fb15d4-1e6d-11ef-8a77-0242ac110002", i['symbol'])
        print(f"{i['symbol']} {count}")


def add_default_symbol(user_id, symbol):
    for i in symbol:
        res = add_new_sub(user_id, i)
        print(res)


def data_to_sso():
    try:
        sql = f"select * from TB_File_Parsing where response is not null limit 1"
        data = query_dict(sql)
        if len(data) > 0:
            data = data[0]
        fh = open(f"/data/bak/report/{data['file_id']}_1.1.txt", 'w', encoding='utf-8')
        fh.write(data['response'])
        fh.close()
        minio_obj = Bucket()
        res1, res2 = None, None
        if minio_obj.exists_bucket("report-parse-result"):
            value_as_bytes = data['result'].encode('utf-8')
            res1 = minio_obj.upload_bytes(bucket_name="report-parse-result", key=f"{data['file_id']}_1.1",
                                          bytes_content=value_as_bytes)
        if minio_obj.exists_bucket("report-parse-response"):
            value_as_bytes = data['response'].encode('utf-8')
            res2 = minio_obj.upload_bytes(bucket_name="report-parse-response",
                                          key=f"{data['file_id']}_1.1",
                                          bytes_content=value_as_bytes)
        del minio_obj
        if res1.object_name and res2.object_name:
            sql = f"update TB_File_Parsing set response=null,result='',version='1.1' where file_id='{data['file_id']}'"
            res = execute(sql)
            print(f"file_id:{data['file_id']} res:{res}")
    except Exception as e:
        print(str(e))


def check_not_title_summary_file():
    pass


def add_all_default_symbol():
    sql = "select * from TB_User where update_time>'2024-05-12'"
    user = query_dict(sql)
    for i in user:
        for h in ['GOOGL.US', 'AAPL.US', 'TSLA.US', 'MSFT.US', 'AMZN.US', 'NVDA.US', 'META.US']:
            sql = "select * from TB_User_Sub where symbol=%s and user_id=%s"
            sub = query_dict(sql, (h, i['uuid']))
            if len(sub) == 0:
                add_new_sub(i['uuid'], h)


def check_wechat_new_user():
    url = "http://212.64.23.164:9530/wechat/friend/get"

    payload = {}
    headers = {
        "Authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MTY1MzY4NDkuMzM2MTAzNCwicGxhdGZvcm0iOiJmYXJnb2luc2lnaHQifQ.7RdWCYB19LXtlSyskqGiB06HD0dwU1tKV6S-chUpjXM",
        "content-type": "application/json"
    }

    response = requests.request("POST", url, json=payload, headers=headers)
    data = json.loads(response.text)
    for i in data:
        if not get_wechat_user(name=str(base64.b64encode(i['NickName'].encode("utf-8"))), sign=None, location=None):
            res = add_wechat_user(name=str(base64.b64encode(i['NickName'].encode("utf-8"))), location="", sign="")
            print(res)


def delete_old_sub_record():
    sql = "select * from TB_User_Sub"
    data = query_dict(sql)
    for i in data:
        sql = f"select * from TB_User where uuid='{i['user_id']}'"
        sub = query_dict(sql)
        if len(sub) == 0:
            sql = f"delete from TB_User_Sub where uuid='{i['uuid']}'"
            print(execute(sql))


def v1_to_v2():
    PATH = f"/home/ibagents/files/research/"
    SecretId = 'AKIDLk5E8nDMGx1rTr21obJp7B3tdQLAOcjb'
    SecretKey = 'zkG87QficM1VjhT2OZVFpAFJEzZyOckn'
    GALPHA_PARSING_VERSION = "1.1"
    for i in range(11, 15):
        sql = f"select * from TB_File_Basic where publish_time<'2024-07-16 00:00:00' and publish_time>'2024-07-15 12:00:00' order by publish_time"
        files = query_dict(sql)
        if len(files) == 0:
            continue
        article = []
        for file in files:
            try:
                if file['article_id'] in article:
                    continue
                else:
                    article.append(file['article_id'])
                
                prepare = True
                if file['parse_status'] == "parse_ok":
                    try:
                        minio_obj = Bucket()
                        res_json = minio_obj.client.get_object("report-parse-result",
                                                               f'{file["uuid"]}_1.1').data.decode(
                            'utf-8')
                        del minio_obj
                    except S3Error as e:
                        add_error_log(f"get object {file['uuid']} fail {e}")
                        set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
                        continue
                    if res_json:
                        config = CosConfig(Region='ap-beijing', SecretId=SecretId, SecretKey=SecretKey, Token=None,
                                           Scheme='https')
                        client = CosS3Client(config)

                        
                        response = client.put_object(
                            Bucket='research1-1328064767',
                            Body=res_json.encode('utf-8'),
                            Key=f"{file['uuid']}_{GALPHA_PARSING_VERSION}.json",
                            EnableMD5=False
                        )
                        if 'ETag' not in response:
                            prepare = False

                if file['local_save_path']:
                    path = file['local_save_path']
                    new_path = f"{PATH}{file['uuid']}.{file['type']}"
                    if os.path.exists(path):
                        shutil.copy(path, new_path)
                    if not os.path.exists(new_path):
                        prepare = False

                if prepare:
                    
                    file_id = file["file_id"]
                    sql = f"select attribute from TB_File where id='{file_id}'"
                    attribute = query_dict(sql)
                    if len(attribute) > 0:
                        attribute = attribute[0]['attribute']
                    else:
                        attribute = None
                    sql = """insert into TB_Research(uuid, publish_time, source, title, author, file_type, download_status,
                            source_url, meta_data, lang, create_time,parse_status) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                          """
                    p_key = f"{file['uuid']}"
                    result = execute(sql, (
                        p_key, file['publish_time'], file['source'], file['title'], file['author'], file['type'],
                        True,
                        file['original_url'], attribute, file['lang'], file['create_time'],
                        file['parse_status']))
                    print(f"{p_key} {result} page {i}")
            except Exception as e:
                print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
                continue


def migrate_v2_meta_data():
    for i in range(0, 60):
        try:
            sql = f"select uuid,meta_data,lang,create_time from TB_Research order by publish_time limit 1000 offset {i * 1000}"
            files = query_dict(sql)
            if len(files) == 0:
                print("finish")
                return
            for file in files:
                meta_data = None
                if file['meta_data']:
                    meta_data = file['meta_data']
                else:
                    sql = f"select file_id from TB_File_Basic where uuid='{file['uuid']}'"
                    basic_file = query_dict(sql)
                    if len(basic_file) > 0:
                        basic_file = basic_file[0]
                        basic_file_id = basic_file["file_id"]
                        sql = f"select attribute from TB_File where id='{basic_file_id}'"
                        attribute = query_dict(sql)
                        if len(attribute) > 0:
                            meta_data = attribute[0]['attribute']
                        else:
                            meta_data = None
                sql = f"insert into TB_Research_Attribute(uuid,research_id,value,lang,attribute,create_time)values(%s,%s,%s,%s,%s,%s)"
                execute(sql, (
                    f'{uuid.uuid1()}', file['uuid'], meta_data, file['lang'], 'meta_data', file['create_time']))
        except Exception as e:
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
            continue


def migrate_v2_quartr_data():
    PATH = f"/home/ibagents/files/research/"
    
    
    
    
    
    
    sql = """
    SELECT t1.* 
    FROM TB_Quartr_Calendar t1
    JOIN (
        SELECT event_id, MAX(create_time) AS max_create_time
        FROM TB_Quartr_Calendar
        GROUP BY event_id
    ) t2
    ON t1.event_id = t2.event_id AND t1.create_time = t2.max_create_time;
    """
    calendar_d = query_dict(sql)
    for c in calendar_d:
        try:
            if c['slides_file_id']:
                sql = "select * from TB_File_Basic where uuid=%s"
                file = query_dict(sql, (c['slides_file_id'],))[0]
                p_key = c['slides_file_id']
                
                prepare = True
                if file['local_save_path']:
                    path = file['local_save_path']
                    new_path = f"{PATH}{file['uuid']}.{file['type']}"
                    if os.path.exists(path):
                        shutil.copy(path, new_path)
                    if not os.path.exists(new_path):
                        prepare = False
                if not prepare:
                    continue
                create_time = datetime.datetime.now()
                if add_research(p_key=p_key,
                                event_id=c['uuid'],
                                business_type="slides",
                                publish_time=file['publish_time'],
                                source='quartr', title=c['event_title'], file_type=file['type'],
                                download_status=True, create_time=create_time,
                                source_url=file['original_url']):
                    add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                           value=c['attribute'],
                                           create_time=create_time)

            if c['report_file_id']:
                sql = "select * from TB_File_Basic where uuid=%s"
                file = query_dict(sql, (c['report_file_id'],))
                if len(file) > 0:
                    file = file[0]
                    p_key = c['report_file_id']
                    
                    prepare = True
                    if file['local_save_path']:
                        path = file['local_save_path']
                        new_path = f"{PATH}{file['uuid']}.{file['type']}"
                        if os.path.exists(path):
                            shutil.copy(path, new_path)
                        if not os.path.exists(new_path):
                            prepare = False
                    if not prepare:
                        continue
                    create_time = datetime.datetime.now()
                    if add_research(p_key=p_key,
                                    event_id=c['uuid'],
                                    business_type="report",
                                    publish_time=file['publish_time'],
                                    source='quartr', title=c['event_title'], file_type=file['type'],
                                    download_status=True, create_time=create_time,
                                    source_url=file['original_url']):
                        add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                               value=c['attribute'],
                                               create_time=create_time)

            if c['audio_file_id']:
                sql = "select * from TB_File_Basic where uuid=%s"
                file = query_dict(sql, (c['audio_file_id'],))
                if len(file) > 0:
                    file = file[0]
                    p_key = c['audio_file_id']
                    if c['transcript_file_id']:
                        sql = "select * from TB_File_Basic where uuid=%s"
                        ts_file = query_dict(sql, (c['transcript_file_id'],))
                        if len(ts_file) > 0:
                            ts_file = ts_file[0]
                            if ts_file['local_save_path']:
                                path = ts_file['local_save_path']
                                new_path = f"{PATH}{file['uuid']}.{ts_file['type']}"
                                if os.path.exists(path):
                                    shutil.copy(path, new_path)
                    
                    prepare = True
                    if file['local_save_path']:
                        path = file['local_save_path']
                        new_path = f"{PATH}{file['uuid']}.{file['type']}"
                        if os.path.exists(path):
                            shutil.copy(path, new_path)
                        if not os.path.exists(new_path):
                            prepare = False
                    if not prepare:
                        continue
                    create_time = datetime.datetime.now()
                    parse_status = None
                    if c['transcript_file_id']:
                        parse_status = 'parse_ok'
                    if add_research(p_key=p_key,
                                    event_id=c['uuid'],
                                    business_type="audio",
                                    publish_time=file['publish_time'],
                                    source='quartr', title=c['event_title'], file_type=file['type'],
                                    download_status=True, create_time=create_time,
                                    source_url=file['original_url'], parse_status=parse_status):
                        add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                               value=c['attribute'],
                                               create_time=create_time)
        except Exception as e:
            print(e)


def delete_v1_vector():
    try:
        for c in range(1, 4):
            time.sleep(2)
            url = "https://ops.fargoinsight.com/console/api/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents"
            querystring = {"page": f"1", "limit": "15", "keyword": "", "fetch": ""}
            headers = {
                "authority": "ops.fargoinsight.com",
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiOTgxZDBkN2EtMDlhNC00MWQwLWEwODYtMzliZmMxMDE3NWVjIiwiZXhwIjoxNzI2NjIyNjcxLCJpc3MiOiJTRUxGX0hPU1RFRCIsInN1YiI6IkNvbnNvbGUgQVBJIFBhc3Nwb3J0In0.Qc9A1me_uOBitQncVKytl2k-rO9aXUaUPrm6d7DBQsk",
                "content-type": "application/json",
                "referer": "https://ops.fargoinsight.com/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents",
                "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "cookie": 'rl_page_init_referrer=RudderEncrypt:U2FsdGVkX19S6e9xlXWFwEUQ+rVZZqDoDQuzf0aFxTA=; rl_page_init_referring_domain=RudderEncrypt:U2FsdGVkX18+c6IQzD9I5m0lXd823rvJWVHwO99JyLA=; rl_anonymous_id=RudderEncrypt:U2FsdGVkX19GycH5ttTOf9afpvMS7IuBPEtm/AY7vgpECWAfzfe5BDqs4HGp1M5sUl+teqdybs7D4Xd6yBTwLQ==; rl_user_id=RudderEncrypt:U2FsdGVkX1+4+Z/XLPEtBO8bdAFEcvvBDXScO0NdOkMQ08aHf4uMHsZfsh/+3FhPjG27BqS0hQ1zZt40uKa44iih7FDmwAkcp7zQWBhVnd1mlJclsMpQ/oaJJFSPgTsrOud7ME4N/zAFxTkJmH2HBmODtSLuUY/OUjA6xjPIvOM=; rl_trait=RudderEncrypt:U2FsdGVkX185bFHkPyDuqGh9T05fVdKkQPZKc+26xGTYFHTi2qx5KL8OXKE51WKbFiq5WsJnbj8VkZhx5yYuplDbDlnUkHDRHVdg2eMQ9mzfsEgvubkT4MIpMWA2NwQaKs53a5VhD58RwIMGOiNyRg==; rl_session=RudderEncrypt:U2FsdGVkX1+h/QBEVBv/uo0ZMt6/71Xyv7u8+IfsoAytyid7SJ4/cO2ZRlEtvX3KWjkUlExFKSeom3BfFt7kZ+/jxoH3AZPgF3xUZB95LVHLFpyMYWYuxIfkt0ZEPFImdSG9qzWs0fhV/AgJVN0maw==; ph_phc_4URIAm1uYfJO7j8kWSe0J8lc8IqnstRLS7Jx8NcakHo_posthog={"distinct_id":"1d4a32d7278814f50c7c2f4b750529a8bf7f19871c046d97a77e2174cace48a5
            }

            response = requests.request("GET", url, headers=headers, params=querystring)
            for i in json.loads(response.text)['data']:
                time.sleep(2)
                if i['created_at'] < 1721033365:
                    print("old")
                    continue
                url = f"http://ops.fargoinsight.com/v1/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents/{i['id']}"
                headers = {"Authorization": "Bearer dataset-Xdljx2aImEN2uvSZE4oAzLZq"}
                response = requests.request("DELETE", url, headers=headers)
                print(response.text)
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


def delete_v2_vector():
    try:
        for c in range(1, 5):
            time.sleep(2)
            url = "https://ops.fargoinsight.com/console/api/datasets/d0b959ce-4a9f-4df8-8347-50adbd68a010/documents"
            querystring = {"page": f"1", "limit": "15", "keyword": "", "fetch": ""}
            headers = {
                "authority": "ops.fargoinsight.com",
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiOTgxZDBkN2EtMDlhNC00MWQwLWEwODYtMzliZmMxMDE3NWVjIiwiZXhwIjoxNzI4ODEyMzk1LCJpc3MiOiJTRUxGX0hPU1RFRCIsInN1YiI6IkNvbnNvbGUgQVBJIFBhc3Nwb3J0In0.RHymtwoEfO7xwFy71vVC4UnM7qEG01jaQDTVnfwrk9o",
                "content-type": "application/json",
                "referer": "https://ops.fargoinsight.com/datasets/d0b959ce-4a9f-4df8-8347-50adbd68a010/documents",
                "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "cookie": 'rl_page_init_referrer=RudderEncrypt:U2FsdGVkX19S6e9xlXWFwEUQ+rVZZqDoDQuzf0aFxTA=; rl_page_init_referring_domain=RudderEncrypt:U2FsdGVkX18+c6IQzD9I5m0lXd823rvJWVHwO99JyLA=; rl_anonymous_id=RudderEncrypt:U2FsdGVkX19GycH5ttTOf9afpvMS7IuBPEtm/AY7vgpECWAfzfe5BDqs4HGp1M5sUl+teqdybs7D4Xd6yBTwLQ==; rl_user_id=RudderEncrypt:U2FsdGVkX1+4+Z/XLPEtBO8bdAFEcvvBDXScO0NdOkMQ08aHf4uMHsZfsh/+3FhPjG27BqS0hQ1zZt40uKa44iih7FDmwAkcp7zQWBhVnd1mlJclsMpQ/oaJJFSPgTsrOud7ME4N/zAFxTkJmH2HBmODtSLuUY/OUjA6xjPIvOM=; rl_trait=RudderEncrypt:U2FsdGVkX185bFHkPyDuqGh9T05fVdKkQPZKc+26xGTYFHTi2qx5KL8OXKE51WKbFiq5WsJnbj8VkZhx5yYuplDbDlnUkHDRHVdg2eMQ9mzfsEgvubkT4MIpMWA2NwQaKs53a5VhD58RwIMGOiNyRg==; rl_session=RudderEncrypt:U2FsdGVkX1+h/QBEVBv/uo0ZMt6/71Xyv7u8+IfsoAytyid7SJ4/cO2ZRlEtvX3KWjkUlExFKSeom3BfFt7kZ+/jxoH3AZPgF3xUZB95LVHLFpyMYWYuxIfkt0ZEPFImdSG9qzWs0fhV/AgJVN0maw==; ph_phc_4URIAm1uYfJO7j8kWSe0J8lc8IqnstRLS7Jx8NcakHo_posthog={"distinct_id":"1d4a32d7278814f50c7c2f4b750529a8bf7f19871c046d97a77e2174cace48a5
            }

            response = requests.request("GET", url, headers=headers, params=querystring)
            for i in json.loads(response.text)['data']:
                time.sleep(2)
                if i['created_at'] < 1726329600:
                    print("old")
                    continue
                url = f"http://ops.fargoinsight.com/v1/datasets/d0b959ce-4a9f-4df8-8347-50adbd68a010/documents/{i['id']}"
                headers = {"Authorization": "Bearer dataset-Xdljx2aImEN2uvSZE4oAzLZq"}
                response = requests.request("DELETE", url, headers=headers)
                print(response.text)
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


def downloadHtml(file_url, local_file_path, header):
    try:
        
        response = requests.get(url=file_url, headers=header)
        
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(bytes(response.text, encoding='utf-8'))
            return True
    except Exception as e:
        print(str(e))


def get_summary(path, research_id, source_url):
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
    if not os.path.exists(path):
        downloadHtml(source_url, f'{"/home/ibagents/files/research/"}{research_id}.html', headers)
    with open(path, 'r', encoding='UTF-8') as f:
        all_content = f.read()
        responseHtml = BeautifulSoup(all_content, 'html.parser')
        summary = responseHtml.find_all(class_="mb-16 text-4x-large-b")
        if len(summary) > 0:
            summary = str(summary[0])
        summary = str(summary)

        summary2 = responseHtml.find_all(class_="mb-24 Dw1An")
        if len(summary2) > 0:
            summary2 = str(summary2)
        summary = summary + summary2
    return summary


def get_sa_summary():
    PATH = f"/home/ibagents/files/research/"
    stock_ticker = ['TSLA', 'MSFT', 'AMZN', 'AAPL', 'TSM', 'AMD', 'META', 'GOOGL', 'PDD', 'COIN', 'GOOG',
                    'ASML', 'LI', 'ARM', 'MSTR', 'CRWD', 'PLTR', 'AVGO', 'MU', 'BABA', 'BIDU', 'ADBE', 'FUTU', 'NIO']
    
    result_dict = dict()
    id_dict = dict()
    for i in stock_ticker:
        sql = f"SELECT uuid,source_url FROM TB_Research WHERE stock_ticker LIKE '%{i}%' AND create_time>'2024-10-01 00:00:00' AND create_time<'2024-10-31 00:00:00' AND SOURCE='sa'"
        data = query_dict(sql)
        for h in data:
            if h['uuid'] in id_dict:
                continue
            id_dict[h['uuid']] = h['source_url']
    for i in id_dict:
        try:
            
            result_dict[i] = html2text.html2text(
                get_summary(path=f"{PATH}{i}.html", research_id=i, source_url=id_dict[i]))
        except Exception as e:
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
    
    for key, value in result_dict.items():
        with open(f'sa_temp/{key}.txt', 'w') as file:
            file.write(f"{value}")
    print("down")


def get_ib_summary():
    PATH = f"/home/ibagents/files/research/"
    stock_ticker = ['TSLA', 'MSFT', 'AMZN', 'AAPL', 'TSM', 'AMD', 'META', 'GOOGL', 'PDD', 'COIN', 'GOOG',
                    'ASML', 'LI', 'ARM', 'MSTR', 'CRWD', 'PLTR', 'AVGO', 'MU', 'BABA', 'BIDU', 'ADBE', 'FUTU', 'NIO']
    
    result_dict = dict()
    with open("ib_data.csv", mode='w', newline='', encoding='utf-8') as file:
        for i in stock_ticker:
            writer = csv.DictWriter(file, fieldnames=['uuid', 'title', 'author', 'publish_time', 'value'])
            sql = f"SELECT a.uuid,a.title,a.author,a.publish_time,b.value FROM TB_Research a left join TB_Research_Attribute b on a.uuid=b.research_id where b.attribute='meta_summary' " \
                  f"and a.stock_ticker LIKE '%{i}%' AND a.create_time>'2024-11-11 00:00:00' AND a.create_time<'2024-11-21 02:00:00' AND a.source in ('jp','ms','gs','ubs')"
            data = query_dict(sql)
            for row in data:
                writer.writerow(row)
    print("down")


if __name__ == '__main__':
    
    
    
    
    
    
    
    get_ib_summary()























