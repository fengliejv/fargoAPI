import json
from time import sleep

import requests


def clean_dataset():
    url = "https://ops.fargoinsight.com/console/api/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents"
    while True:
        querystring = {"page": f"1", "limit": "15", "keyword": "", "fetch": ""}
        headers = {
            "authority": "ops.fargoinsight.com",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiOTgxZDBkN2EtMDlhNC00MWQwLWEwODYtMzliZmMxMDE3NWVjIiwiZXhwIjoxNzE1NDEwNjQwLCJpc3MiOiJTRUxGX0hPU1RFRCIsInN1YiI6IkNvbnNvbGUgQVBJIFBhc3Nwb3J0In0.8BzW8wgVgrtkey61-XUQZ8kI96GaLZYtGIVlvMr7j-U",
            "content-type": "application/json",
            "referer": "https://ops.fargoinsight.com/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents",
            "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        data = json.loads(response.text)
        if not data["has_more"]:
            break
        for i in data['data']:
            url = f"https://ops.fargoinsight.com/console/api/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents/{i['id']}"
            headers = {
                "authority": "ops.fargoinsight.com",
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiOTgxZDBkN2EtMDlhNC00MWQwLWEwODYtMzliZmMxMDE3NWVjIiwiZXhwIjoxNzE1NDEwNjQwLCJpc3MiOiJTRUxGX0hPU1RFRCIsInN1YiI6IkNvbnNvbGUgQVBJIFBhc3Nwb3J0In0.8BzW8wgVgrtkey61-XUQZ8kI96GaLZYtGIVlvMr7j-U",
                "content-type": "application/json",
                "origin": "https://ops.fargoinsight.com",
                "referer": "https://ops.fargoinsight.com/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents",
                "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            }
            response = requests.request("DELETE", url, headers=headers)
            print(response.text)


if __name__ == '__main__':
    clean_dataset()
