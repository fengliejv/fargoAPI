import io
import io
import json

import requests

from service.ReportService import get_question_not_created_uuid_by_source, add_error_log, \
    add_tb_file_basic_summary_question_record

SOURCE = "sa"


def generate_question(uuid, summary, content):
    try:
        url = "http://ops.fargoinsight.com/v1/completion-messages"
        payload = {
            "inputs": {
                "summary": json.dumps(summary),
                "Content": content
            },
            "response_mode": "blocking",
            "user": "abc-123"
        }
        headers = {
            "Authorization": "Bearer app-PPbq1SZhQL81KZ3gUKyLP1i0",
            "Content-Type": "application/json",
            "content-type": "application/json"
        }
        response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
        if response.status_code != 200:
            return False
        data = json.loads(response.text)
        answer = data['answer'].replace('\\n', "").replace("\"", "").strip('`').replace('json', '').strip("\"").replace(
            'plaintext', "").replace(" ", "").strip("\n")
        if answer[0] == "[":
            answer = answer[1:len(answer)]
        if answer[len(answer) - 1] == "]":
            answer = answer[0:len(answer) - 1]
        qs = answer.split(",")
        sn = 1
        for question in qs:
            res = add_tb_file_basic_summary_question_record(file_basic_id=uuid, question=question, sn=sn)
            sn += 1
            if res:
                print("问题插入成功")

    except Exception as e:
        add_error_log(message=f"生成{SOURCE}问题失败,TB_File_Basic_id：{uuid['uuid']}报错:{str(e)}")


def generate_sa_summary_to_question():
    files = get_question_not_created_uuid_by_source(source=SOURCE)
    page = 1
    while len(files) > 0 and page < 200:
        summary = {}
        content = {}
        try:
            for file in files:
                if file['uuid'] in summary:
                    summary[file['uuid']].append(file['summary'])
                else:
                    summary[file['uuid']] = [file['summary']]
            for file in files:
                if file['uuid'] not in content:
                    try:
                        content[file['uuid']] = ''.join(
                            chr(int(file['content'][i:i + 8], 2)) for i in range(0, len(file['content']), 8))
                    except Exception as e:
                        add_error_log(message=f"生成{SOURCE}问题失败,解析content异常,报错:{str(e)}")
                        content[file['uuid']] = ""
                        pass
            for s in summary:
                generate_question(uuid=s, summary=summary[s], content=content[s])
        except Exception as e:
            add_error_log(message=f"生成{SOURCE}问题失败,报错:{str(e)}")
        files = get_question_not_created_uuid_by_source(source=SOURCE)
        page += 1
        print(f"第{page}页")
    return


if __name__ == '__main__':
    generate_sa_summary_to_question()
