import datetime
import json

import requests

from api.v1.service.FileBasicService import get_same_article_info, set_article_info
from api.v1.service.ParsedService import add_original_brief, add_summary
from lib.Common.my_minio import Bucket
from service.FileParsedSummaryService import get_same_article_summary, \
    get_same_article_origin_summary, get_file_summary_limit_recently
from service.ReportService import add_error_log, add_fatal_log

SUMMARY_VERSION = "1.2"


def pre_generate_summary():
    print("pre_generate_summary")
    try:
        
        start_time = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        data = get_file_summary_limit_recently(start_time)
        generated_summary = {}
        print(len(data))
        for i in data:
            try:
                data2 = get_same_article_summary(article_id=i['article_id'], source=i['source'], lang='zh-CN')
                data3 = get_same_article_summary(article_id=i['article_id'], source=i['source'], lang='en-US')
                data4 = get_same_article_origin_summary(article_id=i['article_id'], source=i['source'], lang='en-US')
                data5 = get_same_article_info(article_id=i['article_id'], source=i['source'])
                all_right = True
                if len(data2) > 0:
                    data2 = data2[0]
                    add_summary(file_id=i['uuid'],
                                summary=data2['summary'],
                                lang="zh-CN",
                                version=SUMMARY_VERSION)
                    print(f"same{i['uuid']}")
                else:
                    all_right = False
                if len(data3) > 0:
                    data3 = data3[0]
                    add_summary(file_id=i['uuid'],
                                summary=data3['summary'],
                                lang="en-US",
                                version=SUMMARY_VERSION)
                    print(f"same{i['uuid']}")
                else:
                    all_right = False
                if len(data4) > 0:
                    data4 = data4[0]
                    add_original_brief(file_id=i['uuid'],
                                       summary=data4['summary'],
                                       lang="en-US",
                                       version=SUMMARY_VERSION)
                    print(f"same{i['uuid']}")
                else:
                    all_right = False

                if len(data5) > 0:
                    data5 = data5[0]
                    set_article_info(article_id=i['article_id'], source=i['source'], info=data5['info'])
                    print(f"same{i['uuid']}")
                else:
                    all_right = False
                if all_right:
                    continue

                if f"{i['article_id']}{i['source']}zh_CN" in generated_summary:
                    add_original_brief(file_id=i['uuid'],
                                       summary=generated_summary[f"{i['article_id']}{i['source']}original"],
                                       lang="en-US",
                                       version=SUMMARY_VERSION)
                    add_summary(file_id=i['uuid'],
                                summary=generated_summary[f"{i['article_id']}{i['source']}zh_CN"],
                                lang="zh-CN",
                                version=SUMMARY_VERSION)
                    add_summary(file_id=i['uuid'],
                                summary=generated_summary[f"{i['article_id']}{i['source']}en_US"],
                                lang="en-US",
                                version=SUMMARY_VERSION)
                    set_article_info(article_id=i['article_id'],
                                     source=i['source'],
                                     info=generated_summary[f"{i['article_id']}{i['source']}info"])
                    continue
                
                minio_obj = Bucket()
                minio_obj.client.stat_object("report-parse-result", f"{i['uuid']}_1.1")
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
                add_summary(file_id=i['uuid'],
                            summary=result["Summary_en"],
                            lang="en-US",
                            version=SUMMARY_VERSION)
                add_original_brief(file_id=i['uuid'],
                                   summary=result["Original_Summary"],
                                   lang="en-US",
                                   version=SUMMARY_VERSION)
                set_article_info(article_id=i['article_id'],
                                 source=i['source'],
                                 info=result["meta_info"])
                print(f"generate{i['uuid']}")
            except Exception as e:
                print(f"{str(e)}")
                continue
    except Exception as e:
        print(f"{str(e)}")
        add_fatal_log(message=f"pre_generate_lang_title,fatal:{str(e)}")


if __name__ == '__main__':
    pre_generate_summary()
