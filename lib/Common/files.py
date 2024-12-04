import os
import re

import requests
from lib.Common.utils import make_request
from service.ReportService import add_info_log, add_error_log, add_fatal_log


def downloadImg(url, path, source_url):
    try:
        response = requests.get(source_url + url)
        if response.status_code == 200:
            index = url.rfind("/")
            dir_path = path + url[0:index]
            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)
            save_path = path + url
            with open(save_path, 'wb') as image_file:
                image_file.write(response.content)
            add_info_log(message=f'图片下载成功 {url}')
        else:
            add_error_log(message=f'图片下载失败 {url}')
    except Exception as e:
        add_fatal_log(message=f"下载图片{url}时发生错误:{str(e)}")


def downloadPdf(file_url, local_file_path, data_row, header):
    try:
        response = requests.get(url=file_url, json=data_row, headers=header, timeout=300)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            add_info_log(message=f'文件下载成功 {file_url}')
            print(f'文件下载成功 {file_url}')
            return True
        else:
            add_error_log(message=f"文件下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        add_fatal_log(message=f"下载文件时发生错误:{str(e)}")


def downloadHtml(file_url, local_file_path, data_row, header, path, source_url):
    try:
        response = requests.post(url=file_url, json=data_row, headers=header, timeout=300)
        if response.status_code == 200:
            pattern = r'<img\s+src="([^"]+\.png)"'
            urls = re.findall(pattern, response.text)
            res_save = response.text
            for url in urls:
                if 'https://' not in url:
                    path_change = url.replace(":", "_")
                    res_save = res_save.replace(url, f"..{path_change}")
                    downloadImg(url, path, source_url)
            with open(local_file_path, 'wb') as file:
                file.write(bytes(res_save, encoding='utf-8'))
            add_info_log(message=f"网页下载成功{file_url}")
            print(f'网页下载成功 {file_url}')
            return True
        else:
            add_error_log(message=f"网页下载失败. 响应状态码:{response.status_code}")
    except Exception as e:
        print("下载网页时发生错误:", str(e))
        add_fatal_log(message=f"下载网页时发生错误:{str(e)}")


def research_download(file_url, local_file_path, data_row, header):
    try:
        response = make_request(method='POST', url=file_url, params=data_row, headers=header, retries=3)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            print(f'file download success {file_url}')
            return True
        else:
            add_error_log(message=f"file download fail code:{response.status_code}")
    except Exception as e:
        add_fatal_log(message=f"file download fail coed: {str(e)}", e=e)


def research_download_get(file_url, local_file_path, data_row, header):
    try:
        response = make_request(method='GET', url=file_url, params=data_row, headers=header, retries=3)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            print(f'file download success {file_url}')
            return True
        else:
            add_error_log(message=f"file download fail code:{response.status_code}")
    except Exception as e:
        add_fatal_log(message=f"file download fail coed: {str(e)}", e=e)
    return False


def research_get(file_url, local_file_path, header):
    try:
        response = make_request(method='GET', url=file_url, headers=header, retries=3)
        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            print(f'file download success {file_url}')
            return True
        else:
            add_error_log(message=f"file download fail code:{response.status_code}")
    except Exception as e:
        add_fatal_log(message=f"file download fail coed: {str(e)}", e=e)
