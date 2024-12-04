import datetime
import json
import os
import sys
import threading

import pypdf

sys.path = ['', '/usr/lib/python310.zip', '/usr/lib/python3.10', '/usr/lib/python3.10/lib-dynload',
            '/home/ibagents/.virtualenvs/pythonProject1/lib/python3.10/site-packages',
            '/home/ibagents/.local/lib/python3.10/site-packages', '/usr/local/lib/python3.10/dist-packages',
            '/usr/lib/python3/dist-packages', '/usr/bin']
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from lib.Common.mysqlsingle import execute
import requests
from minio import S3Error
from minio.commonconfig import CopySource
from requests_toolbelt import MultipartEncoder

from api.common.app import query_dict
from lib.Common.my_minio import Bucket
from service.FileBasicService import set_file_basic_attr, check_file_basic_same_file_parsed
from service.GAlphaService import add_parsing_record
from service.ReportService import add_error_log

GALPHA_PARSING_VERSION = "1.1"


class myThread(threading.Thread):  
    def __init__(self, source, start_time, end_time, num):
        threading.Thread.__init__(self)
        self.source = source
        self.start_time = start_time
        self.end_time = end_time
        self.num = num

    def run(self):
        

        sql = f"select uuid,article_id,source,title,local_save_path,create_time,parse_count from TB_File_Basic where publish_time>'{self.start_time}' and publish_time<'{self.end_time} ' " \
              f"and (parse_status='pending' or parse_status is null) and source='{self.source}' order by publish_time desc"
        files = query_dict(sql)
        print(len(files))
        for file in files:
            if int(file['parse_count']) < 3 or int(file['parse_count']) == 3:
                continue
            print(f"{file['uuid']}_{self.num}")
            try:
                
                
                try:
                    same_file = check_file_basic_same_file_parsed(article_id=file['article_id'], source=file['source'],
                                                                  start_time=self.start_time)
                    if len(same_file) > 0:
                        same_file = same_file[0]
                        minio_obj = Bucket()
                        minio_obj.client.copy_object(
                            "report-parse-result",
                            f"{file['uuid']}_{GALPHA_PARSING_VERSION}",
                            CopySource("report-parse-result", f"{same_file['uuid']}_{GALPHA_PARSING_VERSION}"),
                        )
                        del minio_obj
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                        continue
                except S3Error as e:
                    add_error_log(f"{self.source} copy object {file['uuid']}_{GALPHA_PARSING_VERSION} fail {e}")
                url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file"
                header = {}
                with open(file=file['local_save_path'], mode='rb') as fis:
                    file_content = fis
                    file_p = {
                        'filename': file['local_save_path'],
                        'Content-Disposition': 'form-data;',
                        'Content-Type': 'multipart/form-data',
                        'file': (file['local_save_path'], file_content, 'multipart/form-data'),
                        'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"}'
                    }
                    form_data = MultipartEncoder(file_p)  
                    header['content-type'] = form_data.content_type
                    r = requests.post(url, data=form_data, headers=header, timeout=600)

                    if r.status_code != 200:
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                        add_error_log(f"{self.source} Report parsing失败{r.text}")
                    else:
                        data = json.loads(r.text)
                        if "parsed_result" not in data:
                            set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                            continue
                        res = json.loads(data["parsed_result"])
                        res_json = json.dumps(res)
                        add_res = add_parsing_record(file_id=file['uuid'], parsing_platform='galpha',
                                                     req=file_p['file_metadata'],
                                                     result=res_json, article_id=file['article_id'], response=r.text,
                                                     version=GALPHA_PARSING_VERSION)
                        if add_res:
                            set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                            print(f"{self.num} {self.source} success,{file['uuid']}")
                            
                        else:
                            add_error_log(f"{self.source} Report parsing失败")
                            set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                            continue

            except Exception as e:
                print(str(e))
                set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                continue


def run(source, start_time, end_time):
    
    sql = f"select uuid,article_id,source,title,local_save_path,create_time,parse_count,publish_time from TB_File_Basic where publish_time>'{start_time}' and publish_time<'{end_time} ' " \
          f"and (parse_status='pending' or parse_status is null) and source='{source}' order by publish_time desc"
    files = query_dict(sql)
    print(len(files))
    for file in files:
        
        
        
        
        try:
            print(f"{source} {file['uuid']} {file['local_save_path']} {file['publish_time']}")
            try:
                pdf_file = open(file=file['local_save_path'], mode='rb')
                reader = pypdf.PdfReader(pdf_file)
                print(len(reader.pages))
                if len(reader.pages) > 100:
                    continue
                pdf_file.close()
            except Exception as e:
                print(f"{source} {file['article_id']} {file['uuid']}  {file['local_save_path']} {file['publish_time']}")
                sql = "delete from TB_File_Basic where article_id=%s and source=%s"
                execute(sql, (file['article_id'], file['source']))
                continue
            
            try:
                same_file = check_file_basic_same_file_parsed(article_id=file['article_id'], source=file['source'],
                                                              start_time=start_time)
                if len(same_file) > 0:
                    same_file = same_file[0]
                    minio_obj = Bucket()
                    minio_obj.client.copy_object(
                        "report-parse-result",
                        f"{file['uuid']}_{GALPHA_PARSING_VERSION}",
                        CopySource("report-parse-result", f"{same_file['uuid']}_{GALPHA_PARSING_VERSION}"),
                    )
                    del minio_obj
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                    print(f"same {file['uuid']}")
                    continue
            except S3Error as e:
                add_error_log(f"{source} copy object {file['uuid']}_{GALPHA_PARSING_VERSION} fail {e}")
            url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file"
            header = {}
            with open(file=file['local_save_path'], mode='rb') as fis:
                file_content = fis
                file_p = {
                    'filename': file['local_save_path'],
                    'Content-Disposition': 'form-data;',
                    'Content-Type': 'multipart/form-data',
                    'file': (file['local_save_path'], file_content, 'multipart/form-data'),
                    'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"}'
                }
                form_data = MultipartEncoder(file_p)  
                header['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=header, timeout=1200)

                if r.status_code != 200:
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                    add_error_log(f"{source} Report parsing失败{r.text}")
                else:
                    data = json.loads(r.text)
                    if "parsed_result" not in data:
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                        continue
                    res = json.loads(data["parsed_result"])
                    res_json = json.dumps(res)
                    add_res = add_parsing_record(file_id=file['uuid'], parsing_platform='galpha',
                                                 req=file_p['file_metadata'],
                                                 result=res_json, article_id=file['article_id'], response=r.text,
                                                 version=GALPHA_PARSING_VERSION)
                    if add_res:
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                        print(f"{source} success,{file['uuid']}")
                        
                    else:
                        add_error_log(f"{source} Report parsing失败")
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                        continue

        except Exception as e:
            print(str(e))
            set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
            continue


def single_parse():
    for i in ['ms', 'gs', 'ubs']:
        run(i, '2024-01-01 00:00:00', '2024-07-01 00:00:00')


def multi_parse(source):
    i = 1
    thread1 = myThread(source=source, start_time=f'2024-0{i}-01 00:00:00',
                       end_time=f'2024-0{i + 1}-01 00:00:00', num=i)
    i = 2
    thread2 = myThread(source=source, start_time=f'2024-0{i}-01 00:00:00',
                       end_time=f'2024-0{i + 1}-01 00:00:00', num=i)
    i = 3
    thread3 = myThread(source=source, start_time=f'2024-0{i}-01 00:00:00',
                       end_time=f'2024-0{i + 1}-01 00:00:00', num=i)
    i = 4
    thread4 = myThread(source=source, start_time=f'2024-0{i}-01 00:00:00',
                       end_time=f'2024-0{i + 1}-01 00:00:00', num=i)
    i = 5
    thread5 = myThread(source=source, start_time=f'2024-0{i}-01 00:00:00',
                       end_time=f'2024-0{i + 1}-01 00:00:00', num=i)

    
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()


if __name__ == '__main__':
    
    single_parse()
