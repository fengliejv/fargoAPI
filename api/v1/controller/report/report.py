import datetime
import threading
import time
import json
import uuid
import deepl
from flask import Blueprint
from api.v1.lib.Jwt import check_token
from api.v1.service.GAlphaService import get_parsing_record, set_parsed_file_status
from api.v1.service.ParsedService import get_not_generation_summary_file_title, set_file_basic_status
from api.v1.service.QuestionService import add_file_question
from api.v1.service.ReportService import add_fatal_log, get_title_lang, get_research_list
from minio import S3Error
from api.v1.service.FileBasicService import set_file_basic_attr, set_article_info
from api.v1.lib.common.my_minio import Bucket
from api.v1.lib.common.utils import generate_random_string, is_json
from api.v1.service.ParsedService import add_summary, add_original_brief
from api.v1.lib.common.utils import report_resort
from flask import send_file, make_response, request
import requests
from api.v1.service.ParsedOriginalSummaryService import get_original_summary
from requests_toolbelt import MultipartEncoder
from api.v1.service.QuestionService import get_file_question, add_file_question_uuid
from api.v1.service.ReportService import get_question_by_question_id, add_question, get_platform_user_id, \
    get_file_path_by_file_id, get_file_by_file_id, get_file_basic_by_file_id, add_error_log, get_file_tickers, \
    add_answer, \
    get_parsed_summary_lang, add_title, get_titles, get_report_question_by_fileid, get_user_report_question, \
    set_file_code, search_file_code, get_attribute_by_download_file_id, set_file_param, get_file_basic_embedding, \
    get_parsed_summary_by_article_id, get_files_brief
from api.v1.service.SystemService import get_system_variable
from api.v1.service.APIUserService import get_preference, set_preference
from api.v1.service.GAlphaService import add_parsing_record

GALPHA_PARSING_VERSION = "1.1"
SUMMARY_VERSION = "1.2"
DIFY_REPOSITORY = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
DATASET_ID = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
LOG_TAG = "sys report api"
report_controller = Blueprint('report_controller', __name__)
parsing_file_id = []


@report_controller.route('/v1/report/summary/add', methods=['POST'])
def summary_add():
    SUMMARY_VERSION = "1.2"
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        original = req_data.get('Original_Summary')
        summary_en = req_data.get('Summary_en')
        summary_cn = req_data.get('Summary_cn')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        add_res = True
        if not add_summary(file_id=file_id,
                           summary=summary_cn,
                           lang="zh-CN",
                           version=SUMMARY_VERSION):
            add_res = False
        if not add_summary(file_id=file_id,
                           summary=summary_en,
                           lang="en-US",
                           version=SUMMARY_VERSION):
            add_res = False
        if not add_original_brief(file_id=file_id,
                                  summary=original,
                                  lang="en-US",
                                  version=SUMMARY_VERSION):
            add_res = False
        if add_res:
            res['status'] = True
        else:
            res['err_msg'] = "add fail"
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/title/add', methods=['POST'])
def title_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        title = req_data.get('title')
        lang = req_data.get('lang')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        add_res = True
        if not add_title(file_id=file_id, title=title, lang=lang):
            add_res = False
        if add_res:
            res['status'] = True
        else:
            res['err_msg'] = "add fail"
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/preference/get', methods=['POST'])
def preference_get():
    res = {
        'data': {},
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        user_id = req_data.get('user_id')
        if not user_id:
            res["err_msg"] = "user_id is null"
            return res
        data = get_preference(platform=token_dict[1]['platform'], user_id=user_id)
        if len(data) > 0:
            res['data'] = json.loads(data[0]['preference'])
            res['status'] = True
        else:
            res["err_msg"] = "user not found"
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/preference/set', methods=['POST'])
def preference_set():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        user_id = req_data.get('user_id')
        preference = req_data.get('preference')
        if not user_id:
            res["err_msg"] = "user_id is null"
            return res
        if not preference:
            res["err_msg"] = "preference is null"
            return res
        set_preference(preference=json.dumps(preference), platform=token_dict[1]['platform'], user_id=user_id)
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/content', methods=['POST'])
def report_content():
    res = {
        'status': False,
        'data': '',
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        file = get_file_basic_by_file_id(file_id)
        if len(file) == 0:
            res["err_msg"] = "file not found"
            return res

        parsing_text = ""
        
        minio_obj = Bucket()
        res_json = minio_obj.client.get_object("report-parse-result",
                                               f'{file_id}_{GALPHA_PARSING_VERSION}').data.decode('utf-8')
        del minio_obj
        parsing_data = json.loads(res_json)
        parsing_data = report_resort(parsing_data)
        for key in list(parsing_data.keys()):
            parsing_text = parsing_text + "\n\n" + parsing_data[key]['data']
        res["data"] = parsing_text
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/original_summary', methods=['POST'])
def report_original_summary():
    res = {
        'status': False,
        'data': '',
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        file = get_original_summary(file_id, version="v2.0")
        if len(file) == 0:
            res["err_msg"] = "original_summary not found"
            return res

        res["data"] = file[0]["summary"]
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/format', methods=['POST'])
def report_format():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        content = report_resort(req_data)
        res['data'] = content
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)


@report_controller.route('/v1/report/list', methods=['POST'])
def report_list():
    res = {
        'status': False,
        'data': [],
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        start_time = req_data.get('start_time')
        end_time = req_data.get('end_time')
        page_size = req_data.get('page_size')
        page_count = req_data.get('page_count')

        if not end_time:
            end_time = "2030-01-01 00:00:00"
        if not start_time:
            start_time = "2020-01-01 00:00:00"
        if page_size > 100:
            page_size = 100
        
        files = get_research_list(start_time=start_time, end_time=end_time, page_size=page_size, page_count=page_count)
        for i in files:
            if i['download_status']:
                i['url'] = f"https://files.fargoinsight.com/research/file/{i['uuid']}.{i['file_type']}"
            res['data'].append(i)
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/question', methods=['POST'])
def report_question():
    res = {
        'data': {},
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        user_id = req_data.get('user_id')
        question_id = req_data.get('question_id')
        lang = req_data.get('lang')
        if not lang:
            lang = "en-US"
        
        data = None
        if question_id and not user_id and not file_id:
            data = get_question_by_question_id(question_id=question_id)
        if user_id and file_id:
            data = get_user_report_question(files_id=file_id, user_id=user_id)
        elif file_id:
            data = get_report_question_by_fileid(files_id=file_id)
            if len(data) == 0:
                version = "v2.0"
                en_summary = get_parsed_summary_lang(file_id=file_id, lang="en-US", version=version)
                deepl_glossary_id = get_system_variable(key="deepl_glossary_id")
                deepl_key = get_system_variable(key="deepl_key")
                if not (deepl_glossary_id and deepl_key):
                    res["err_msg"] = "deepl_glossary_id wrong"
                    return res
                translator = deepl.Translator(deepl_key[0]["value"])
                version = "v2.0"
                en_question = get_file_question(files_id=file_id, lang="en-US", version=version, user_id="system")

                if len(en_question) == 0:
                    title = get_file_by_file_id(file_id=file_id)
                    if len(title) > 0:
                        title = title[0]["title"]
                    else:
                        title = " "
                    url = "http://ops.fargoinsight.com/v1/workflows/run"
                    payload = {
                        "inputs": {
                            "report_title": title,
                            "report_id": file_id,
                            "task_type": "questions"
                        },
                        "response_mode": "blocking",
                        "user": "abc-123"
                    }
                    headers = {
                        "Authorization": "Bearer app-XptU90g4vMSvjCEAQkFXbZtd",
                        "Content-Type": "application/json",
                        "content-type": "application/json"
                    }
                    response = requests.request("POST", url, json=payload, headers=headers, timeout=(300.0, 600.0))
                    if response.status_code != 200:
                        print(f"三藏摘要workflow调用失败")
                        add_error_log(f"三藏摘要workflow调用失败{response.text}")
                    result = json.loads(response.text)['data']['outputs']
                    question = result['text']
                    
                    question = question.replace('\\n', "").replace("\"", "").strip('`').replace('json', '').strip(
                        "\"").replace('plaintext', "").strip("\n")
                    if question[0] == "[":
                        question = question[1:len(question)]
                    if question[len(question) - 1] == "]":
                        question = question[0:len(question) - 1]
                    qs = question.split(",")
                    questions = []
                    for q in qs:
                        en_question = {}
                        en_question_uuid = uuid.uuid1()
                        add_res = add_file_question_uuid(question_uuid=en_question_uuid, files_id=file_id, question=q,
                                                         lang="en-US", version=version,
                                                         user_id="system")
                        en_question["question_id"] = en_question_uuid
                        en_question["question_body"] = q
                        en_question["question_lang"] = "en-US"
                        en_question["user_id"] = "system"
                        en_question["create_time"] = datetime.datetime.now()
                        en_question["files_id"] = file_id
                        questions.append(en_question)
                        if add_res:
                            print(f"en-US问题插入成功{q}")
                        translate_res = translator.translate_text(text=q, glossary=deepl_glossary_id[0]["value"],
                                                                  target_lang="ZH", source_lang="EN")
                        q = translate_res.text.replace('\\n', "")
                        cn_question_uuid = uuid.uuid1()
                        add_res = add_file_question_uuid(question_uuid=cn_question_uuid, files_id=file_id, question=q,
                                                         lang="zh-CN", version=version,
                                                         user_id="system")
                        if add_res:
                            print(f"cn问题插入成功，问题：{translate_res}")
                        cn_question = {}
                        cn_question["question_id"] = cn_question_uuid
                        cn_question["question_body"] = q
                        cn_question["question_lang"] = "zh-CN"
                        cn_question["create_time"] = datetime.datetime.now()
                        cn_question["user_id"] = "system"
                        cn_question["files_id"] = file_id
                        questions.append(cn_question)
                    res["data"] = questions
                    res['status'] = True
                    return res
        questions = []
        for i in data:
            question = {}
            question["question_id"] = i["uuid"]
            question["question_body"] = i["question"]
            question["question_lang"] = i["lang"]
            question["user_id"] = i["user_id"]
            question["files_id"] = i["files_id"]
            question["create_time"] = i["create_time"]
            questions.append(question)
        res["data"] = questions
        res['status'] = True
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/download')
def download_report():
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        path = get_file_path_by_file_id(request.args.get('file_id'))
        if not path:
            return json.dumps({"err_msg": "file_id not exist"})
        path = path[0]["local_save_path"]
        response = make_response(send_file(path, as_attachment=True))
        filename = path[path.rfind("/") + 1:len(path)]
        response.headers["Content-Disposition"] = f"attachment;filename={filename}"
        return response
    except Exception as e:
        return str(e)


@report_controller.route('/v1/report/preview')
def preview_report():
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        path = get_file_path_by_file_id(request.args.get('file_id'))
        if not path:
            return json.dumps({"err_msg": "file_id not exist"})
        path = path[0]["local_save_path"]
        response = make_response(send_file(path, as_attachment=False))
        filename = path[path.rfind("/") + 1:len(path)]
        response.headers["Content-Disposition"] = f"inline;filename={filename}"
        return response
    except Exception as e:
        return str(e)


@report_controller.route('/v1/report/detail')
def report_detail():
    data = {
        "fileId": "",
        "code": "",
        "reportType": "",
        "publishCode": "",
        "publishDate": "",
        "reportTitle": "",
        "reportBrief": "",
        "reportTitleResource": {},
        "reportBriefResource": {},
        "tickers": "",
        "handleStatus": ""
    }
    res = {
        "status": False,
        "data": data,
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        req_lang = request.args.get('lang')
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        file = get_file_by_file_id(request.args.get('file_id'))
        if len(file) == 0:
            return res
        else:
            file = file[0]
        if 'node' in request.args.keys():
            node = request.args.get('node')
            if node == 'attribute':
                down_load = get_attribute_by_download_file_id(file_id=file["file_id"])
                if len(down_load) > 0:
                    attribute = json.loads(down_load[0]['attribute'])
                    if file["source"] == 'ms' and "reports" in attribute:
                        data['attribute'] = json.loads(down_load[0]['attribute'])['reports'][0]
                    else:
                        data['attribute'] = attribute
        data["fileId"] = file["uuid"]
        data["reportType"] = "COM"
        data['parseStatus'] = file['parse_status']
        data['embeddingStatus'] = file['embedding_status']
        data["publishCode"] = file["source"]
        data["publishDate"] = file["publish_time"]
        data["reportTitle"] = file["title"]
        if is_json(file["info"]):
            data["info"] = json.loads(file["info"])
        else:
            data["info"] = None
        data["reportTitleResource"][f"i18n-en-US"] = file["title"]
        summaries = get_parsed_summary_by_article_id(file['article_id'])
        titles = get_titles(file['uuid'])
        if not req_lang:
            req_lang = "en-US"
        for s in summaries:
            data["reportBriefResource"][f"i18n-{s['lang']}"] = s["summary"]
            if s["lang"] == req_lang:
                data["reportBrief"] = s["summary"]

        for t in titles:
            data["reportTitleResource"][f"i18n-{t['lang']}"] = t["title"]
            if t["lang"] == req_lang:
                data["reportTitle"] = t["title"]
        data['tickers'] = ",".join(get_file_tickers(file["article_id"], file["source"]))
        if not file['code']:
            for h in range(0, 10):
                g_code = generate_random_string()
                if len(search_file_code(g_code)) > 0:
                    continue
                else:
                    s_res = set_file_code(article_id=file["article_id"], source=file["source"], code=g_code)
                    if s_res:
                        data['code'] = g_code
                    else:
                        res['err_msg'] = 'generate code fail'
                        return res
                    break
        else:
            data['code'] = file['code']
        res['data'] = data
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/reports/brief', methods=['POST'])
def reports_detail():
    res = {
        "status": False,
        "data": [],
        "err_msg": ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        files_id = req_data.get('files_id')
        params = req_data.get('params')
        file = get_files_brief(files_id=files_id)
        if len(file) == 0:
            return res
        info_params = ["content_type", "report_type", "primary_symbol"]
        for i in file:
            temp = {}
            temp['file_id'] = i['uuid']
            for h in params:
                if h not in info_params:
                    temp[h] = i[h]
                    continue
                temp[h] = None
                if not i['info']:
                    continue
                info_dict = json.loads(i['info'])
                if h in info_dict:
                    temp[h] = info_dict[h]
            res['data'].append(temp)
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/update', methods=['POST'])
def report_update():
    status = False
    err_msg = ""
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        file = get_file_by_file_id(file_id=file_id)
        if len(file) == 0:
            return json.dumps({
                'status': status,
                'err_msg': "file not found"
            })
        if 'attribute' in req_data:
            attribute = req_data.get('attribute')
            attribute = json.loads(attribute)
            attribute = json.dumps(attribute)
            res = set_file_param(param='attribute', value=attribute, file_path=file[0]['local_save_path'])
            if res:
                return json.dumps({
                    'status': True,
                    'err_msg': ""
                })
            else:
                return json.dumps({
                    'status': status,
                    'err_msg': "update fail"
                })
    except Exception as e:
        err_msg = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=err_msg, source=LOG_TAG, e=e)
    return json.dumps({
        'status': status,
        'err_msg': err_msg
    })



@report_controller.route('/v1/report/new/summary')
def new_summary():
    res = {
        'data': "",
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        file = get_file_basic_by_file_id(request.args.get('file_id'))
        if len(file) == 0:
            res["err_msg"] = "file not found"
            return res
        file = file[0]
        minio_obj = Bucket()
        minio_obj.client.stat_object("report-parse-result", f"{file['uuid']}_1.1")
        url = "http://ops.fargoinsight.com/v1/workflows/run"
        payload = {
            "inputs": {
                "report_title": file['title'],
                "report_id": file['uuid'],
                "task_type": "summary"
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
            res['err_msg'] = response.text
            return res
        result = json.loads(response.text)['data']['outputs']['result']
        result["Summary_cn"] = str(result["Summary_cn"])
        result["Summary_en"] = str(result["Summary_en"])
        result["Original_Summary"] = str(result["Original_Summary"])
        result["meta_info"] = json.dumps(result["meta_info"])
        add_summary(file_id=file['uuid'],
                    summary=result["Summary_cn"],
                    lang="zh-CN",
                    version=SUMMARY_VERSION)
        add_summary(file_id=file['uuid'],
                    summary=result["Summary_en"],
                    lang="en-US",
                    version=SUMMARY_VERSION)
        add_original_brief(file_id=file['uuid'],
                           summary=result["Original_Summary"],
                           lang="en-US",
                           version=SUMMARY_VERSION)
        set_article_info(article_id=file['article_id'],
                         source=file['source'],
                         info=result["meta_info"])
        res['data'] = result
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/embedding', methods=['POST'])
def report_embedding():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        DIFY_TOKEN = get_system_variable("dify_token")
        if len(DIFY_TOKEN) > 0:
            DIFY_TOKEN = DIFY_TOKEN[0]["value"]
        else:
            res["err_msg"] = "dify token expire"
            return res
        file = get_file_basic_embedding(file_id=file_id)
        if len(file) == 0:
            res["err_msg"] = "file not found"
            return json.dumps(res)
        file = file[0]
        if file['embedding_status'] == 'embedding_ok':
            res['status'] = True
            return res
        file_res = search_dify_file(file['uuid'])
        if file_res['total'] > 0:
            dify_file_id = file_res['data'][0]['id']
            if file_res['data'][0]['indexing_status'] == "completed":
                set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_ok')
                res['status'] = True
                return res
        else:
            url = "https://ops.fargoinsight.com/console/api/files/upload?source=datasets"
            querystring = {"source": "datasets"}
            path = file['local_save_path']
            with open(file=path, mode='rb') as fis:
                headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Authorization": f"Bearer {DIFY_TOKEN}",
                    "Connection": "keep-alive",
                    "Content-Type": "multipart/form-data; boundary=---011000010111000001101001",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-site",
                    "content-type": "multipart/form-data"
                }
                file_content = fis
                file_p = {
                    'file': (file['uuid'] + ".pdf", file_content, 'application/pdf'),
                }
                form_data = MultipartEncoder(file_p)  
                headers['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=headers, params=querystring, timeout=500)
                data = json.loads(r.text)
                if not data['id']:
                    set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
                    add_error_log(f"sync_embedding upload pdf file fail {r.text}")
                    res['err_msg'] = "upload dify fail"
                    return res
                else:
                    dify_file_id = data['id']
        try:
            minio_obj = Bucket()
            res_json = minio_obj.client.get_object("report-parse-result",
                                                   f'{file["uuid"]}_{file["version"]}').data.decode(
                'utf-8')
            del minio_obj
        except S3Error as e:
            add_error_log(f"get object {file['uuid']} fail {e}")
            set_file_basic_attr(file['uuid'], 'embedding_status', 'parse_fail')
            res['err_msg'] = "oss not found file"
            return res
        if res_json:
            embedding_res = dify_embedding(dify_file_id, file['uuid'], json.loads(res_json), DIFY_TOKEN)
            if embedding_res:
                set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_ok')
                res['status'] = True
            else:
                set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
                res['err_msg'] = "dify embedding api fail"
                return res
        else:
            set_file_basic_attr(file['uuid'], 'embedding_status', 'embedding_fail')
            res['err_msg'] = "oss file is null"
            return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


def dify_embedding(upload_id: str, file_id: str, parsing_data: dict, dify_token: str):
    result = False
    try:
        parsing_data = report_resort(parsing_data)
        upload_data = {}
        parsing_text = ""
        for key in list(parsing_data.keys()):
            upload_data[key] = parsing_data[key]
            parsing_text = parsing_text + " " + parsing_data[key]['data']
        datas = {
            "file_id": file_id,
            "parsing_data": upload_data
        }
        url = f'https://ops.fargoinsight.com/console/api/datasets/{DIFY_REPOSITORY}/documents/custom'

        payload = {
            "data_source": {
                "type": "upload_file",
                "info_list": {
                    "data_source_type": "upload_file",
                    "file_info_list": {"file_ids": [upload_id]}
                }
            },
            "indexing_technique": "high_quality",
            "process_rule": {
                "rules": {},
                "mode": "automatic"
            },
            "doc_form": "text_model",
            "doc_language": "Chinese",
            "retrieval_model": {
                "search_method": "hybrid_search",
                "reranking_enable": True,
                "reranking_model": {
                    "reranking_provider_name": "jina",
                    "reranking_model_name": "jina-reranker-v1-base-en"
                },
                "top_k": 3,
                "score_threshold_enabled": False,
                "score_threshold": 0.5
            },
            "custom": datas
        }
        headers = {
            "authority": "ops.fargoinsight.com",
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "content-type": "application/json",
            "origin": "https://ops.fargoinsight.com",
            "referer": "https://ops.fargoinsight.com/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents/create",
            "Authorization": f"Bearer {dify_token}"}

        response = requests.request("POST", url, json=payload, headers=headers)
        if response.status_code != 200:
            add_error_log(message=f"dify embedding api error:{response.text}")
            return False
        for i in range(0, 60):
            time.sleep(1)
            
            res = search_dify_file(file_id=file_id)
            if res['total'] == 0:
                continue
            if res['data'][0]['indexing_status'] == "completed":
                return True
            time.sleep(5)
    except Exception as e:
        add_error_log(e=e, message=f"dify embedding api fail:{str(e)}")
    return result


def search_dify_file(file_id):
    result = False
    url = f"http://ops.fargoinsight.com/v1/datasets/{DATASET_ID}/documents?keyword={file_id}.pdf"
    headers = {"Authorization": "Bearer dataset-Xdljx2aImEN2uvSZE4oAzLZq"}
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        add_error_log(f"dify search file fail{response.text}")
    else:
        result = json.loads(response.text)
    return result


@report_controller.route('/v1/report/parse', methods=['POST'])
def report_parse():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        file_id = req_data.get('file_id')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        file = get_file_by_file_id(file_id=file_id)
        if len(file) == 0:
            res["err_msg"] = "file not found"
            return json.dumps(res)
        file = file[0]
        if file['parse_status'] == "parse_ok":
            res['status'] = True
            return res
        url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file"
        header = {}
        with open(file=file['local_save_path'], mode='rb') as fis:
            file_content = fis
            file_p = {
                'filename': file['local_save_path'],
                'Content-Disposition': 'form-data;',
                'Content-Type': 'multipart/form-data',
                'file': (file['local_save_path'], file_content, 'multipart/form-data'),
                'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"}'
            }
            form_data = MultipartEncoder(file_p)  
            header['content-type'] = form_data.content_type
            r = requests.post(url, data=form_data, headers=header, timeout=300)

            if r.status_code != 200:
                set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                print(f"Report parsing失败")
                add_error_log(f"{file_id} Report parsing失败: {r.text}")
                res['err_msg'] = f"galpha parse_fail {r.text}"
                return res
            else:
                data = json.loads(r.text)
                if "parsed_result" not in data:
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                    res['err_msg'] = "galpha parse_fail"
                    return res
                parsed_result = json.loads(data["parsed_result"])
                res_json = json.dumps(parsed_result)
                add_res = add_parsing_record(file_id=file['uuid'], parsing_platform='galpha',
                                             req=file_p['file_metadata'],
                                             result=res_json, article_id=file['article_id'], response=r.text,
                                             version=GALPHA_PARSING_VERSION)
                if add_res:
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_ok')
                    print(f"success,{file['uuid']}")
                    res['status'] = True
                else:
                    add_error_log(f"{file_id} Report parsing失败")
                    set_file_basic_attr(file['uuid'], 'parse_status', 'parse_fail')
                    res['err_msg'] = f"add parse record fail"
                    return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@report_controller.route('/v1/report/ask', methods=['POST'])
def ask_report():
    app_key = "app-RbA2guFSgaQNC0CuMEtX3X90"
    res = {
        'status': False,
        'data': {},
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        question_id = req_data.get('question_id')
        report_id = req_data.get('report_id')
        question_body = req_data.get('question_body')
        user_id = req_data.get('user_id')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        user_id = get_platform_user_id(user_id, token_dict[1]['platform'])

        if question_id:
            question = get_question_by_question_id(question_id)
            if len(question) == 0:
                res['err_msg'] = "questionId not exist"
                return res
            else:
                return res
        question_id = uuid.uuid1()
        result = add_question(question_id=question_id, user_id=user_id, reports_id=report_id, question=question_body,
                              source=token_dict[1]['platform'])
        if result:
            url = "http://ops.fargoinsight.com/v1/completion-messages"
            payload = {
                "inputs": {
                    "docid": report_id,
                    "question": question_body
                },
                "response_mode": "blocking",
                "user": "system"
            }
            headers = {
                "Authorization": f"Bearer {app_key}",
                "Content-Type": "application/json",
                "content-type": "application/json"
            }
            response = requests.request("POST", url, json=payload, headers=headers, timeout=(120.0, 300.0))
            if response.status_code != 200:
                print(f"fail")
                add_error_log(f"answer api fail{response.text}")
            answer = json.loads(response.text)['answer']
            res['data'] = answer
            add_res = add_answer(question_id=question_id, answer=answer, lang="zh-CN", source=f"dify:{app_key}")
            if add_res:
                print(f"summary: {answer}")
            res["status"] = True
            return res
        else:
            res["err_msg"] = "save question fail"
            return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)



@report_controller.route('/v1/report/handle')
def report_handle():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        file_id = request.args.get('file_id')
        if not file_id:
            return json.dumps({'status': False, "err_msg": "file_id is null"})
        if file_id in parsing_file_id:
            return json.dumps({'status': False, "err_msg": "file is parsing now"})
        threading.Thread(target=parsing_report, args=(file_id,)).start()
        res["status"] = True
        parsing_file_id.append(file_id)
        return res
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


def parsing_report(file_id):
    print(file_id)
    res = {
        'status': False,
        'err_msg': ""
    }
    DIFY_REPOSITORY = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
    res_json = ""
    parsing_text = ""
    try:
        DIFY_TOKEN = get_system_variable("dify_token")
        if len(DIFY_TOKEN) > 0:
            DIFY_TOKEN = DIFY_TOKEN[0]["value"]
        else:
            res["err_msg"] = "dify token expire"
            return res

        file = get_file_basic_by_file_id(file_id)
        if len(file) == 0:
            res["err_msg"] = "file not found"
            return res
        file = file[0]
        file_content = open(file=file['local_save_path'], mode='rb')
        async_record = get_parsing_record(article_id=file['article_id'], version_id=GALPHA_PARSING_VERSION)
        if len(async_record) > 0:
            res_json = async_record[0]['result']
            if res_json == '':
                minio_obj = Bucket()
                res_json = minio_obj.client.get_object("report-parse-result",
                                                       f'{file["uuid"]}_{GALPHA_PARSING_VERSION}').data.decode('utf-8')
                del minio_obj
            parsing_data = json.loads(res_json)
            parsing_data = report_resort(parsing_data)
            for key in list(parsing_data.keys()):
                parsing_text = parsing_text + " " + parsing_data[key]['data']
        else:
            url = "https://test.generativealpha.ai/doc_analysis/upload_pdf_file"
            header = {}
            file_p = {
                'filename': file['local_save_path'],
                'Content-Disposition': 'form-data;',
                'Content-Type': 'multipart/form-data',
                'file': (file['local_save_path'], file_content, 'multipart/form-data'),
                'file_metadata': '{"need_parsing_result": "True", "organization": "EasyView"}'
            }
            form_data = MultipartEncoder(file_p)  
            header['content-type'] = form_data.content_type
            r = requests.post(url, data=form_data, headers=header, timeout=1200)

            if r.status_code != 200:
                res["err_msg"] = "Report parsing fail"
                print(f"parsing fail")
                add_fatal_log(f"parsing fail")
                return res

            data = json.loads(r.text)
            if "parsed_result" not in data:
                res["err_msg"] = "Report parsing null"
                return res
            result = json.loads(data["parsed_result"])
            res_json = json.dumps(result)
            add_parsing_record(file_id=file['uuid'], parsing_platform='galpha', req=file_p['file_metadata'],
                               result=res_json, article_id=file['article_id'], response=r.text,
                               version=GALPHA_PARSING_VERSION)
            print(f"success,{file['uuid']}")

        async_record = get_parsing_record(article_id=file['article_id'], version_id=GALPHA_PARSING_VERSION)
        if len(async_record) > 0:
            temp = int.from_bytes(async_record[0]["upload_status"], byteorder='big')
            if not temp:
                url = "https://ops.fargoinsight.com/console/api/files/upload?source=datasets"
                querystring = {"source": "datasets"}
                path = file['local_save_path']
                headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Authorization": f"Bearer {DIFY_TOKEN}",
                    "Connection": "keep-alive",
                    "Content-Type": "multipart/form-data; boundary=---011000010111000001101001",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-site",
                    "content-type": "multipart/form-data"
                }
                file_p = {
                    'file': (file_id + ".pdf", file_content, 'application/pdf'),
                }
                form_data = MultipartEncoder(file_p)  
                headers['content-type'] = form_data.content_type
                r = requests.post(url, data=form_data, headers=headers, params=querystring)
                data = json.loads(r.text)
                if "id" not in data:
                    res["err_msg"] = "file to dify fail"
                    return res
                parsing_data = json.loads(res_json)
                parsing_data = report_resort(parsing_data)
                upload_data = {}
                parsing_text = ""
                for key in list(parsing_data.keys()):
                    upload_data[key] = parsing_data[key]
                    parsing_text = parsing_text + " " + parsing_data[key]['data']
                datas = {
                    "file_id": file['uuid'],
                    "parsing_data": upload_data
                }
                url = f'https://ops.fargoinsight.com/console/api/datasets/{DIFY_REPOSITORY}/documents/custom'

                payload = {
                    "data_source": {
                        "type": "upload_file",
                        "info_list": {
                            "data_source_type": "upload_file",
                            "file_info_list": {"file_ids": [data['id']]}
                        }
                    },
                    "indexing_technique": "high_quality",
                    "process_rule": {
                        "rules": {},
                        "mode": "automatic"
                    },
                    "doc_form": "text_model",
                    "doc_language": "Chinese",
                    "retrieval_model": {
                        "search_method": "semantic_search",
                        "reranking_enable": True,
                        "reranking_model": {
                            "reranking_provider_name": "jina",
                            "reranking_model_name": "jina-reranker-v1-base-en"
                        },
                        "top_k": 3,
                        "score_threshold_enabled": False,
                        "score_threshold": 0.5
                    },
                    "custom": datas
                }
                headers = {
                    "authority": "ops.fargoinsight.com",
                    "accept": "*/*",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "content-type": "application/json",
                    "origin": "https://ops.fargoinsight.com",
                    "referer": "https://ops.fargoinsight.com/datasets/87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38/documents/create",
                    "Authorization": f"Bearer {DIFY_TOKEN}"}

                response = requests.request("POST", url, json=payload, headers=headers)

                print(response.text)
                if response.status_code == 200:
                    set_parsed_file_status(file_id=file['uuid'])
        else:
            res["err_msg"] = "parsed data not found"
            return res

        
        version = "v2.0"
        en_summary = get_parsed_summary_lang(file_id=file['uuid'], lang="en-US", version=version)
        summary = ""
        deepl_glossary_id = get_system_variable(key="deepl_glossary_id")
        deepl_key = get_system_variable(key="deepl_key")
        if not (deepl_glossary_id and deepl_key):
            res["err_msg"] = "deepl_glossary_id wrong"
            return res
        translator = deepl.Translator(deepl_key[0]["value"])
        if len(en_summary) == 0:
            title = get_not_generation_summary_file_title(file['uuid'])
            if len(title) > 0:
                title = title[0]["title"]
            else:
                title = " "
            url = "http://ops.fargoinsight.com/v1/workflows/run"
            payload = {
                "inputs": {
                    "report_title": title,
                    "report_id": file_id,
                    "task_type": "summary"
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
                print(f"success")
                add_error_log(f"summary fail {response.text}")
            result = json.loads(response.text)['data']['outputs']['result']
            summary = result["Summary"]
            original_summary = result["Original_Summary"]
            add_res = add_summary(file_id=file['uuid'], summary=summary, lang="en-US", version=version)
            add_original_brief(file_id=file['uuid'], summary=original_summary, lang="en-US", version=version)
            if add_res:
                print(f"insert success：{summary}")
            translate_res = translator.translate_text(text=summary, glossary=deepl_glossary_id[0]["value"],
                                                      target_lang="ZH", source_lang="EN")
            add_res = add_summary(file_id=file['uuid'], summary=translate_res.text, lang="zh-CN", version=version)
            if add_res:
                print(f"insert success：{summary}")
        else:
            summary = en_summary[0]['summary']

        version = "v2.0"
        en_question = get_file_question(files_id=file_id, lang="en-US", version=version, user_id="system")

        if len(en_question) == 0:
            title = get_file_by_file_id(file_id=file_id)
            if len(title) > 0:
                title = title[0]["title"]
            else:
                title = " "
            url = "http://ops.fargoinsight.com/v1/workflows/run"
            payload = {
                "inputs": {
                    "report_title": title,
                    "report_id": file_id,
                    "task_type": "questions"
                },
                "response_mode": "blocking",
                "user": "abc-123"
            }
            headers = {
                "Authorization": "Bearer app-XptU90g4vMSvjCEAQkFXbZtd",
                "Content-Type": "application/json",
                "content-type": "application/json"
            }
            response = requests.request("POST", url, json=payload, headers=headers, timeout=(300.0, 600.0))
            if response.status_code != 200:
                print(f"fail")
                add_error_log(f"summary fail {response.text}")
            result = json.loads(response.text)['data']['outputs']
            question = result['text']
            question = question.replace('\\n', "").replace("\"", "").strip('`').replace('json', '').strip(
                "\"").replace('plaintext', "").strip("\n")
            if question[0] == "[":
                question = question[1:len(question)]
            if question[len(question) - 1] == "]":
                question = question[0:len(question) - 1]
            qs = question.split(",")
            for q in qs:
                add_res = add_file_question(files_id=file_id, question=q, lang="en-US", version=version,
                                            user_id="system")
                if add_res:
                    translate_res = translator.translate_text(text=q, glossary=deepl_glossary_id[0]["value"],
                                                              target_lang="ZH", source_lang="EN")
                q = translate_res.text.replace('\\n', "")
                add_res = add_file_question(files_id=file_id, question=q, lang="zh-CN", version=version,
                                            user_id="system")
                if add_res:
                    pass

        
        cn_title = get_title_lang(file_id=file_id, lang="zh-CN")
        if len(cn_title) == 0:
            translate_res = translator.translate_text(text=file['title'], glossary=deepl_glossary_id[0]["value"],
                                                      target_lang="ZH", source_lang="EN")
            add_res = add_title(file_id=file_id, title=translate_res.text, lang="zh-CN")
            if add_res:
                pass
        set_res = set_file_basic_status(file_id=file_id, status=True)
        if set_res == 1 or bool(int.from_bytes(file["handle_status"], byteorder='big')):
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    parsing_file_id.remove(file_id)
    print(res)
    return res
