import datetime

import requests

import urllib.parse
from urllib.parse import unquote
import os

from service.ReportService import add_file_record


def extract_filename_from_url(url):
    url = unquote(url)
    parsed_url = urllib.parse.urlparse(url)
    path = parsed_url.path
    filename = path.split('/')[-1]
    return filename


def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def download_from_google(file_url, local_file_path):
    try:
        response = requests.get(url=file_url)
        ensure_directory_exists(local_file_path)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            print(f'文件下载成功 {file_url}')
            return True
        else:
            print(response.text)
    except Exception as e:
        print(str(e))


def run():
    file = open("07-10上传研报.csv", "r")  
    content = file.readlines()  
    report_arr = []
    for line in content:  
        line = line.replace('"', '')
        line = line.strip('\n')
        temp = line.split(",")
        report_arr.append(temp)
    for report in report_arr:
        try:
            source = report[0]
            publish_time = report[1]
            url = report[2]
            title = report[3]
            name = extract_filename_from_url(url)
            PATH = f"/home/ibagents/files/{source.lower()}"
            dir_name = PATH + "/" + "private"
            res = download_from_google(url, dir_name + '/' + name)
            if res:
                add_file_record(type="pdf",
                                file_path=dir_name + '/' + name,
                                title=title,
                                source=url,
                                attribute="{}",
                                publish_time=datetime.datetime.strptime(publish_time, '%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


if __name__ == '__main__':
    run()
