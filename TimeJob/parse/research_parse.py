import datetime
import json

import requests
from qcloud_cos import CosConfig, CosS3Client
from requests_toolbelt import MultipartEncoder

from service.ReportService import add_error_log, add_fatal_log
from service.ResearchService import get_research_need_parse2, set_research_attr

GALPHA_PARSING_VERSION = "1.1"
SecretId = 'AKIDLk5E8nDMGx1rTr21obJp7B3tdQLAOcjb'
SecretKey = 'zkG87QficM1VjhT2OZVFpAFJEzZyOckn'
PATH = f"/home/ibagents/files/research/"
IB_SOURCE = ['ubs', 'ms', 'gs', 'cicc', 'jp']
PRIVATE_SOURCE=['pando','trading desk']

def research_parse():
    # start_time = datetime.datetime.now() - datetime.timedelta(hours=24)
    start_time = datetime.datetime.now() - datetime.timedelta(hours=240)
    # files = get_research_need_parse(start_time=start_time)
    files = get_research_need_parse2(start_time=start_time)
    print(len(files))
    config = CosConfig(Region='ap-beijing', SecretId=SecretId, SecretKey=SecretKey, Token=None,
                       Scheme='https')
    client = CosS3Client(config)
    temp = []
    for file in files:
        if not file['download_status']:
            continue
        if file['source'] in IB_SOURCE and file['file_type'] == 'pdf':
            temp.append(file)
            continue
        if file['source'] in PRIVATE_SOURCE and file['file_type'] == 'pdf':
            temp.append(file)
            continue
        if file['source'] == 'quartr' and file['business_type'] == 'slides':
            continue
        if file['source'] == 'quartr' and file['business_type'] == 'audio':
            continue
        if file['source'] == 'quartr' and file['business_type'] == 'report':
            temp.append(file)
    files = temp
    for file in files:
        set_research_attr(file['uuid'], 'parse_status', 'parsing')
    for file in files:
        attribute = file['meta_data']
        if not attribute:
            attribute = "{}"
        try:
            url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file/v2"
            header = {}
            local_path = f"{PATH}{file['uuid']}.{file['file_type']}"
            with open(file=local_path, mode='rb') as fis:
                file_content = fis
                file_p = {
                    'filename': f"{file['uuid']}.pdf",
                    'Content-Disposition': 'form-data;',
                    'Content-Type': 'multipart/form-data',
                    'file': (local_path, file_content, 'multipart/form-data'),
                    'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"' + f',"attribute":{attribute}' + '}'
                }
                form_data = MultipartEncoder(file_p)  
                header['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=header, timeout=600)

                if r.status_code != 200:
                    set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
                    add_error_log(f"research parsing fail {r.text}")
                else:
                    data = json.loads(r.text)
                    if "parsed_result" not in data:
                        set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
                        continue
                    res = json.loads(data["parsed_result"])
                    for h in res:
                        if 'attribute' in res[h]['metadata']:
                            res[h]['metadata'].pop('attribute')
                        if 'text' in res[h]['metadata']:
                            res[h]['metadata'].pop('text')
                    res_json = json.dumps(res)

                    
                    response = client.put_object(
                        Bucket='research1-1328064767',
                        Body=res_json.encode('utf-8'),
                        Key=f"{file['uuid']}_{GALPHA_PARSING_VERSION}.json",
                        EnableMD5=False
                    )

                    if 'ETag' in response:
                        set_research_attr(file['uuid'], 'parse_status', 'parse_ok')
                        print(f"success,{file['uuid']}")
                        
                    else:
                        add_error_log(f"research parsing {file['uuid']}")
                        set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
                        continue

        except Exception as e:
            print(str(e))
            set_research_attr(file['uuid'], 'parse_status', 'parse_fail')
            add_fatal_log(e=e, message=f"research parsing fail:{str(e)}")
            continue


if __name__ == '__main__':
    research_parse()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
