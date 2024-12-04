import datetime
import json

import requests
from minio import S3Error
from minio.commonconfig import CopySource
from requests_toolbelt import MultipartEncoder

from lib.Common.my_minio import Bucket
from service.FileBasicService import get_file_basic_need_parse, set_file_basic_attr, check_file_basic_same_file_parsed
from service.FileService import get_file_attribute
from service.GAlphaService import add_parsing_record, add_parsed_record
from service.ReportService import add_error_log, add_fatal_log

GALPHA_PARSING_VERSION = "1.1"


def sync_parsing(source):
    
    start_time = datetime.datetime.now() - datetime.timedelta(weeks=3)
    files = get_file_basic_need_parse(start_time=start_time, source=source)
    print(len(files))
    for file in files:
        print(f"{file['uuid']}")
        if int(file['parse_count']) > 3:
            continue
        attribute = {}
        try:
            set_file_basic_attr(file['uuid'], 'parse_status', 'parsing')
            
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
                    add_res = add_parsed_record(file_id=file['uuid'], parsing_platform='galpha',
                                                req="",
                                                article_id=file['article_id'],
                                                version=GALPHA_PARSING_VERSION)
                    if add_res:
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                        print(f"success,{file['uuid']}")
                        
                    else:
                        add_error_log(f"{source} Report parsing失败")
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                    continue
            except S3Error as e:
                add_error_log(f"{source} copy object {file['uuid']}_{GALPHA_PARSING_VERSION} fail {e}")
            try:
                data = get_file_attribute(file_id=file['file_id'])
                if len(data) > 0:
                    attribute = data[0]['attribute']
            except Exception as e:
                add_error_log(f"{source} copy object {file['uuid']}_{GALPHA_PARSING_VERSION} fail {e}")
            url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file/v2"
            header = {}
            with open(file=file['local_save_path'], mode='rb') as fis:
                file_content = fis
                file_p = {
                    'filename': file['local_save_path'],
                    'Content-Disposition': 'form-data;',
                    'Content-Type': 'multipart/form-data',
                    'file': (file['local_save_path'], file_content, 'multipart/form-data'),
                    'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"' + f',"attribute":{json.dumps(attribute)}' + '}'
                }
                form_data = MultipartEncoder(file_p)  
                header['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=header, timeout=600)

                if r.status_code != 200:
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                    print(f"Report parsing失败")
                    add_error_log(f"{source} Report parsing失败{r.text}")
                else:
                    data = json.loads(r.text)
                    if "parsed_result" not in data:
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                        continue
                    res = json.loads(data["parsed_result"])
                    res_json = json.dumps(res)
                    add_res = add_parsing_record(file_id=file['uuid'], parsing_platform='galpha',
                                                 req="",
                                                 result=res_json, article_id=file['article_id'], response=r.text,
                                                 version=GALPHA_PARSING_VERSION)
                    if add_res:
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                        print(f"success,{file['uuid']}")
                        
                    else:
                        add_error_log(f"{source} Report parsing失败")
                        set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                        continue

        except Exception as e:
            print(str(e))
            set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
            add_fatal_log(e=e, message=f"{source} Report Parsing失败,报错:{str(e)}")
            continue


if __name__ == '__main__':
    sync_parsing(source="ubs")
