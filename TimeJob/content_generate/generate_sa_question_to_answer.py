import json

import requests

from service.ReportService import add_error_log, get_question_not_answered_by_source, add_question_answer_record

SOURCE = "sa"

def generate_answer(uuid, question, content):
    try:
        if not question:
            return False
        url = "http://ops.fargoinsight.com/v1/completion-messages"
        payload = {
            "inputs": {
                "Question": question,
                "Content": content
            },
            "response_mode": "blocking",
            "user": "abc-123"
        }
        headers = {
            "Authorization": "Bearer app-Rv280n7rbuWnqLJf9wYm1GWB",
            "Content-Type": "application/json",
            "content-type": "application/json"
        }
        response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
        if response.status_code != 200:
            add_error_log(f"fail")
        answer = json.loads(response.text)['answer']
        res = add_question_answer_record(file_basic_content_summary_question_id=uuid, answer=answer)
        if res:
            pass

    except Exception as e:
        add_error_log(
            message=f"生成{SOURCE}问题答案失败,file_basic_content_summary_question_id：{uuid['uuid']}报错:{str(e)}")


def generate_sa_question_to_answer():
    files = get_question_not_answered_by_source(source=SOURCE)
    page = 1
    while len(files) > 0 and page < 200:
        question = {}
        content = {}
        try:
            for file in files:
                question[file['uuid']] = file['question']
            for file in files:
                if file['uuid'] not in content:
                    try:
                        content[file['uuid']] = ''.join(
                            chr(int(file['content'][i:i + 8], 2)) for i in range(0, len(file['content']), 8))
                    except Exception as e:
                        add_error_log(message=f"生成{SOURCE}问题失败,解析content异常,报错:{str(e)}")
                        content[file['uuid']] = ""
                        pass
            for q in question:
                generate_answer(uuid=q, question=question[q], content=content[q])
        except Exception as e:
            add_error_log(message=f"生成{SOURCE}问题失败,报错:{str(e)}")
        files = get_question_not_answered_by_source(source=SOURCE)
    page += 1
    print(f"第{page}页")


if __name__ == '__main__':
    generate_sa_question_to_answer()
