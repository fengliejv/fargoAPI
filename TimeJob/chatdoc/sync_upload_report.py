import json
from datetime import datetime

import requests

from service.ChatdocService import get_not_upload_file, add_upload_record
from service.ReportService import add_error_log, add_fatal_log

PATH = f"/home/ibagents/files/"


def sync_upload_report():
    files = get_not_upload_file()
    for file in files:
        try:
            url = 'https://api.chatdoc.site/api/v2/documents/upload'
            with open(file['local_save_path'], 'rb') as f:
                files = {'file': f}
                headers = {
                    "Authorization": "Bearer ak-NXHDf3dQSGTZwr4iqvDGcJF-DOeZ9OEuI6-rDctzV6U",
                    "content-type": "multipart/form-data"
                }
                response = requests.request('POST', url, files=files, headers=headers, timeout=(120.0, 500.0))
            if response.status_code != 200:
                add_fatal_log(f"chatdoc fail")
            else:
                
                add_upload_record(file['uuid'], json.loads(response.text)['data']['id'])
                print(f"success,chatdoc upload{file['uuid']}")
        except Exception as e:
            print(str(e))
            add_error_log(message=f"chatdoc上传失败,报错:{str(e)}")


if __name__ == '__main__':
    sync_upload_report()
