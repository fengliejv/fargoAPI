import datetime
import json

import requests

from api.v1.service.ParsedService import add_summary
from service.ReportService import add_error_log, add_fatal_log
from service.ResearchService import get_research_need_preprocess

SUMMARY_VERSION = "1.2"


def preprocess_research():
    print("preprocess_research")
    try:
        
        start_time = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        
        research = get_research_need_preprocess(start_time)

        generated_summary = {}
        print(len(research))
        for i in research:
            url = "http://ops.fargoinsight.com/v1/workflows/run"
            payload = {
                "inputs": {
                    "report_title": i['title'],
                    "report_id": i['uuid'],
                    "task_type": ""
                },
                "response_mode": "blocking",
                "user": "system"
            }
            headers = {
                "Authorization": "Bearer app-XptU90g4vMSvjCEAQkFXbZtd",
                "Content-Type": "application/json",
                "content-type": "application/json"
            }
            response = requests.request("POST", url, json=payload, headers=headers, timeout=(300.0, 600.0))
            if response.status_code != 200:
                add_error_log(f"fail {response.text}")
                continue
            result = json.loads(response.text)['data']['outputs']['result']
            result["Summary_cn"] = str(result["Summary_cn"])
            result["Summary_en"] = str(result["Summary_en"])
            result["Original_Summary"] = str(result["Original_Summary"])
            result["meta_info"] = json.dumps(result["meta_info"])
            generated_summary[f"{i['article_id']}{i['source']}zh_CN"] = result["Summary_cn"]
            generated_summary[f"{i['article_id']}{i['source']}en_US"] = result["Summary_en"]
            generated_summary[f"{i['article_id']}{i['source']}original"] = result["Original_Summary"]
            generated_summary[f"{i['article_id']}{i['source']}info"] = result["meta_info"]
            add_summary(file_id=i['uuid'],
                        summary=result["Summary_cn"],
                        lang="zh-CN",
                        version=SUMMARY_VERSION)
    except Exception as e:
        print(f"{str(e)}")
        add_fatal_log(message=f"pre_generate_lang_title,fatal:{str(e)}")


if __name__ == '__main__':
    preprocess_research()
