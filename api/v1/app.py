import base64
import datetime
import os
import re
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api.v1.service.ResearchService import add_research, add_research_attribute
from api.v1.controller.dify.dify import dify_controller
from api.v1.controller.common.Sub import sub_controller
from api.v1.controller.common.Msg import msg_controller
from api.v1.service.QuartrCalendarService import get_all_quartr, get_all_quartr_by_symbol, get_all_quartr_by_event_id
from api.v1.yida.bank_ltv import bank_ltv_controller
from api.v1.service.MsgHistoryService import add_send_msg
from api.v1.lib.common.utils import generate_random_string, get_file_extension
from api.v1.service.WechatService import get_wechat_user, add_new_sub
from api.v1.service.WechatBotService import add_wechat_user
from api.v1.lib.common.pypdf_replace import pypdf_replace
from werkzeug.utils import secure_filename
from multiprocessing import cpu_count, Process
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from api.v1.service.TickerService import add_ticker, add_ticker_name, get_company_by_name, get_ticker_by_source_symbol
from flask import Flask
from gevent import pywsgi
from flask import request
import requests
from lib.Jwt import check_token
from api.v1.service.ReportService import add_error_log, get_search_file, get_file_symbols, set_file_code, \
    search_file_code
from api.v1.controller.common.User import user_controller
from api.v1.controller.common.Symbol import symbol_controller
from api.v1.research.research import research_controller
from api.v1.controller.common.ASR import asr_controller
from api.v1.controller.common.Common import common_controller
from api.v1.controller.report.report import report_controller

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
GALPHA_PARSING_VERSION = "1.1"
SUMMARY_VERSION = "1.2"
executor = ThreadPoolExecutor(max_workers=4)  
parsing_file_id = []
DIFY_REPOSITORY = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
DATASET_ID = "87e5a8e5-0839-4cb1-8a2d-ee6cf9522e38"
LOG_TAG = "sys core api"
app.register_blueprint(bank_ltv_controller)
app.register_blueprint(msg_controller)
app.register_blueprint(sub_controller)
app.register_blueprint(user_controller)
app.register_blueprint(symbol_controller)
app.register_blueprint(research_controller)
app.register_blueprint(asr_controller)
app.register_blueprint(dify_controller)
app.register_blueprint(common_controller)
app.register_blueprint(report_controller)


@app.route('/v1/ticker/add', methods=['POST'])
def ticker_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        code = req_data.get('company_code')
        ticker = req_data.get('ticker')
        platform = req_data.get('platform')
        name = req_data.get('company_name')
        company_id = req_data.get('company_id')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        
        get_ticker = get_ticker_by_source_symbol(symbol=ticker, source=platform)
        if len(get_ticker) > 0:
            res['err_msg'] = "ticker exist"
            return res
        if company_id:
            add_res = add_ticker_name(platform=platform, company_code=code, company_id=company_id,
                                      symbol=ticker,
                                      creator=token_dict[1]['platform'], company_name=name)
        else:
            company = get_company_by_name(name)
            if len(company) > 0:
                add_res = add_ticker_name(platform=platform, company_code=code, company_id=company[0]['uuid'],
                                          symbol=ticker,
                                          creator=token_dict[1]['platform'], company_name=name)
            else:

                add_res = add_ticker(platform=platform, company_code=code, company_name=name, symbol=ticker,
                                     creator=token_dict[1]['platform'])
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@app.route('/v1/msg/add', methods=['POST'])
def msg_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        source = req_data.get('source')
        msg_from = req_data.get('from')
        msg_to = req_data.get('to')
        msg = req_data.get('msg')
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            res["err_msg"] = "user unauthorized"
            return json.dumps(res)
        if add_send_msg(source=source, msg_from=msg_from, msg_to=msg_to, msg=msg):
            res['status'] = True

        
        if source == "wechat":
            if not get_wechat_user(name=str(base64.b64encode(msg_from.encode("utf-8"))), sign=None, location=None):
                add_wechat_user(name=str(base64.b64encode(msg_from.encode("utf-8"))), location="", sign="")
                
                user = get_wechat_user(name=str(base64.b64encode(msg_from.encode("utf-8"))), sign=None, location=None)
                if len(user) > 0:
                    for i in ['GOOGL.US', 'AAPL.US', 'TSLA.US', 'MSFT.US', 'AMZN.US', 'NVDA.US', 'META.US']:
                        add_new_sub(user[0]['uuid'], i)

    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


def run_web(MULTI_PROCESS, port):
    if MULTI_PROCESS == False:
        pywsgi.WSGIServer(('0.0.0.0', port), app).serve_forever()
    else:
        mulserver = pywsgi.WSGIServer(('0.0.0.0', port), app)
        mulserver.start()

        def server_forever():
            mulserver.start_accepting()
            mulserver._stop_event.wait()

        for i in range(cpu_count() * 2):
            p = Process(target=server_forever)
            p.start()


@app.route('/v1/system/cpu', methods=['GET'])
def get_cpu_count():
    return f"{cpu_count()}"


@app.route('/v1/system/pdf/replace', methods=['POST'])
def report_replace():
    res = {
        'status': False,
        'err_msg': "",
        'data': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        file_url = None
        filename = f"{uuid.uuid1()}.pdf"
        temp = request.headers['Content-Type']
        if request.headers['Content-Type'] == 'application/json':
            req_data = request.get_json()
            replace_dict_req = json.loads(req_data.get("replace_dict"))
            file_url = req_data.get("file_url")
        else:
            if request.form.get("file_name"):
                filename = request.form.get("file_name")
            replace_dict_req = json.loads(request.form.get("replace_dict"))
        result_path = None
        
        replace_dict = {}
        for key in replace_dict_req:
            temp = key.replace("_", " ").replace("-", " ").replace(".", " ")
            temp = re.sub(r'\s+', ' ', temp).strip()
            s_key = temp.split(" ")
            s_value = replace_dict_req[key].split(" ")
            if replace_dict_req[key] == "" and " " in temp:
                for i in s_key:
                    replace_dict[i] = "*" * len(i)
            elif " " in temp and len(s_key) == len(s_value):
                for i in range(0, len(s_key)):
                    replace_dict[s_key[i]] = s_value[i]
            elif " " in temp and not (len(s_key) == len(s_value)):
                for i in range(0, len(s_key)):
                    replace_dict[s_key[i]] = "*" * len(s_key[i])
            else:
                replace_dict[key] = replace_dict_req[key]
        if file_url:
            response = requests.get(url=file_url, timeout=300)
            if response.status_code == 200:
                with open(f"/home/ibagents/files/replace_file/{filename}", 'wb') as file:
                    file.write(response.content)
                result_path = pypdf_replace(f"/home/ibagents/files/replace_file/{filename}",
                                            replace_dict)
            else:
                res["err_msg"] = "file download fail"
        else:
            file = request.files['file']
            if not file:
                res["err_msg"] = "file not found"
                return res
            filename = secure_filename(file.filename)
            file.save(os.path.join("/home/ibagents/files/replace_file/", filename))
            result_path = pypdf_replace(os.path.join("/home/ibagents/files/replace_file/", filename),
                                        replace_dict)
        if result_path:
            res['status'] = True
            res['data'] = "https://files.fargoinsight.com/pdf/replace/" + filename + ".pdf"
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)


@app.route('/v1/file/upload', methods=['POST'])
def file_upload():
    res = {
        'status': False,
        'err_msg': "",
        'data': {}
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({"err_msg": "user unauthorized"})
        file_url = None
        p_key = f"{uuid.uuid1()}"
        temp = request.headers['Content-Type']
        params = None
        if request.headers['Content-Type'] == 'application/json':
            params = request.get_json()
        else:
            params = request.form
        result_path = None
        
        source_url = params.get('file_url', '')
        file_type = ''
        title = params.get('title', '')
        if source_url:
            file_type = get_file_extension(source_url)
            response = requests.get(url=source_url, timeout=300)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '').split(';')[0]
                mime_to_extension = {
                    'application/pdf': 'pdf',
                    'text/html': 'html',
                    'image/jpeg': 'jpg',
                    'image/png': 'png',
                    'application/msword': 'doc',
                    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
                    'application/vnd.ms-excel': 'xls',
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
                    'text/plain': 'txt',
                }
                file_type = mime_to_extension.get(content_type, '')
                filename = f"{p_key}.{file_type}"
                with open(f"/home/ibagents/files/research/{filename}", 'wb') as file:
                    file.write(response.content)
            else:
                res["err_msg"] = "file download fail"
        else:
            file = request.files['file']
            if not file:
                res["err_msg"] = "file not found"
                return res
            filename = secure_filename(file.filename)
            file.save(os.path.join("/home/ibagents/files/research/", filename))
        parse_status = None
        if file_type == 'html':
            parse_status = 'parse_ok'
        
        create_time = datetime.datetime.now()
        if 'meta_data' in params and params['meta_data']:
            add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=p_key, attribute="meta_data",
                                   value=params['meta_data'],
                                   create_time=datetime.datetime.now())
        if add_research(p_key=p_key,
                        publish_time=datetime.datetime.now(),
                        parse_status=parse_status,
                        business_type='',
                        source=params['source'], title=title,
                        download_status=True, create_time=create_time,
                        file_type=file_type, source_url=source_url):
            res['status'] = True
            res['data']['research_id'] = p_key
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return json.dumps(res)


@app.route('/v1/file/search', methods=['POST'])
def file_search():
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
        title = req_data.get('title')
        source = req_data.get('source')
        symbol = req_data.get('symbol')
        count = req_data.get("count")
        type = req_data.get("type")
        code = req_data.get("code")
        if not end_time:
            end_time = "2030-01-01 00:00:00"
        if not start_time:
            start_time = "2020-01-01 00:00:00"
        if count:
            count = count + 20
        files = get_search_file(start_time=start_time, end_time=end_time, title=title, source=source, symbol=symbol,
                                count=count, code=code, type=type)
        result = []
        res_count = 1
        for i in files:
            if i['article_id'] in result:
                continue
            else:
                if count:
                    if res_count > (count - 20):
                        break
                    res_count = res_count + 1
                result.append(i['article_id'])
            if not i['code'] and type == "pdf":
                
                for h in range(0, 10):
                    g_code = generate_random_string()
                    if len(search_file_code(g_code)) > 0:
                        continue
                    else:
                        s_res = set_file_code(article_id=i['article_id'], source=i['source'], code=g_code)
                        if s_res:
                            i['code'] = g_code
                        else:
                            res['err_msg'] = 'generate code fail'
                            return res
                        break

            symbol = []
            symbols = get_file_symbols(article_id=i['article_id'], source=i['source'])
            for s in symbols:
                symbol.append(s['symbol'])
            i['symbol'] = symbol
            i['url'] = f"https://files.fargoinsight.com/files/{i['uuid']}.{i['type']}"
            i.pop('local_save_path')
            res['data'].append(i)
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@app.route('/v1/quartr/calendar/search', methods=['POST'])
def quartr_calendar_search():
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
        symbol = req_data.get('symbol')
        event_id = req_data.get('event_id')
        data = None
        if not end_time:
            end_time = (datetime.datetime.today() + datetime.timedelta(weeks=2)).strftime("%Y-%m-%d")
        if not start_time:
            start_time = datetime.datetime.today().strftime("%Y-%m-%d")
        if event_id:
            data = get_all_quartr_by_event_id(event_id=event_id)
        elif symbol:
            data = get_all_quartr_by_symbol(start_time=start_time,
                                            end_time=end_time, symbol=symbol)
        elif not symbol:
            data = get_all_quartr(start_time=start_time, end_time=end_time)
        
        
        
        res['data'] = data
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


if __name__ == "__main__":
    port = 9528
    print(app.url_map)
    if os.path.isfile('/.dockerenv'):
        port = 9529
        
        
        run_web(MULTI_PROCESS=True, port=port)
    else:
        run_web(MULTI_PROCESS=False, port=port)
