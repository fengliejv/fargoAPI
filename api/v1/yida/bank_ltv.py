import datetime
import json
import threading

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.BankLtvService import get_all_bank_ltv, get_bank_ltv_search, get_bank_ltv_search_symbol
from api.v1.service.LtvBankService import add_ltv_bank, update_ltv_bank, get_ltv_bank_by_code, get_ltv_bank_all, \
    del_ltv_bank, add_bank_ltv
from api.v1.service.ReportService import add_error_log
from api.v1.yida.bank_ltv_sync import SyncBankLtv

LOG_TAG = "sys ltv bank api"
bank_ltv_controller = Blueprint('bank_ltv_controller', __name__)


@bank_ltv_controller.route('/v1/service/ltv/bank/add', methods=['POST'])
def ltv_bank_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        nodes = req_data.get('nodes')
        bank_name = req_data.get('bank_name')
        add_res = add_ltv_bank(bank_code=bank_code, nodes=nodes, bank_name=bank_name)
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/service/ltv/bank/del', methods=['POST'])
def ltv_bank_del():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        get_res = get_ltv_bank_by_code(bank_code=bank_code)
        if len(get_res) == 0:
            res['err_msg'] = 'bank_code not found'
            return res
        add_res = del_ltv_bank(bank_code=bank_code, bank_uuid=get_res[0]['uuid'])
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/service/ltv/bank/update', methods=['POST'])
def ltv_bank_update():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        bank_name = req_data.get('bank_name')
        add_res = update_ltv_bank(bank_code=bank_code, bank_name=bank_name)
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/service/ltv/bank/get', methods=['POST'])
def ltv_bank_get():
    res = {
        'status': False,
        'data': {},
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        if bank_code:
            res['data'] = get_ltv_bank_by_code(bank_code=bank_code)
        else:
            res['data'] = get_ltv_bank_all()
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/bank/ltv/sync', methods=['POST'])
def bank_ltv_sync():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        nodes = req_data.get('nodes')
        bank_name = req_data.get('bank_name')
        add_res = add_ltv_bank(bank_code=bank_code, nodes=nodes, bank_name=bank_name)
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/bank/ltv/add', methods=['POST'])
def bank_ltv_add():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.form
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        add_res = add_bank_ltv(bank=req_data['bank'],
                               country=req_data['country'],
                               isin=req_data['isin'],
                               underlying=req_data['underlying'],
                               stock_code=req_data['stock_code'],
                               bank_ltv=req_data['bank_ltv'],
                               submitter=req_data['submitter'],
                               effect_time=req_data['effect_time'],
                               create_time=req_data['create_time'])
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/bank/ltv/del')
def bank_ltv_del():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        get_res = get_ltv_bank_by_code(bank_code=bank_code)
        if len(get_res) == 0:
            res['err_msg'] = 'bank_code not found'
            return res
        add_res = del_ltv_bank(bank_code=bank_code, bank_uuid=get_res[0]['uuid'])
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/bank/ltv/update')
def bank_ltv_update():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        bank_name = req_data.get('bank_name')
        add_res = update_ltv_bank(bank_code=bank_code, bank_name=bank_name)
        if add_res:
            res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/bank/ltv/get')
def bank_ltv_get():
    res = {
        'status': False,
        'data': {},
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        if bank_code:
            res['data'] = get_ltv_bank_by_code(bank_code=bank_code)
        else:
            res['data'] = get_ltv_bank_all()
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res


@bank_ltv_controller.route('/v1/bank/ltv/search', methods=['POST'])
def bank_ltv_search():
    res = {
        'status': False,
        'data': {},
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        bank_code = req_data.get('bank_code')
        symbol = req_data.get('symbol')
        start_time = req_data.get('start_time')
        end_time = req_data.get('end_time')
        if bank_code:
            start_time = int(datetime.datetime.strptime(start_time, "%Y-%m-%d").timestamp() * 1000)
            end_time = int(datetime.datetime.strptime(end_time, "%Y-%m-%d").timestamp() * 1000)
            res['data'] = get_bank_ltv_search(bank_code, symbol, start_time, end_time)
            res['status'] = True
            return res
        if len(req_data) == 0:
            data = get_all_bank_ltv()
            bank_dict = {}
            data_dict = {}
            for i in data:
                record = {
                    "date": datetime.datetime.fromtimestamp(i['quote_time'] / 1000).strftime("%Y-%m-%d"),
                    "ltv": i['ltv'],
                    "stock": {
                        "code": i['symbol'],
                        "cnName": i['cn_bank_name'],
                        "name": i['en_bank_name'],
                        "isin": i['isin'],
                        "market": {
                            "name": i['country_code'],
                            "slug": i['exchanger']
                        }
                    },
                    "changed": 0
                }
                if i['bank_code'] not in bank_dict:
                    bank_dict[i['bank_code']] = {}
                    bank_dict[i['bank_code']]['bankCode'] = i['bank_code']
                    bank_dict[i['bank_code']]['enBankName'] = i['en_bank_name']
                    bank_dict[i['bank_code']]['stockltvrecordSet'] = []

                bank_dict[i['bank_code']]['stockltvrecordSet'].append(record)
            res['data'] = list(bank_dict.values())
        if not bank_code and symbol:
            res['data'] = get_bank_ltv_search_symbol(symbol)
        elif bank_code:
            start_time = int(datetime.datetime.strptime(start_time, "%Y-%m-%d").timestamp() * 1000)
            end_time = int(datetime.datetime.strptime(end_time, "%Y-%m-%d").timestamp() * 1000)
            res['data'] = get_bank_ltv_search(bank_code, symbol, start_time, end_time)

        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res

@bank_ltv_controller.route('/v1/yida/sync', methods=['POST'])
def yida_sync():
    res = {
        'status': False,
        'data': {},
        'err_msg': ""
    }
    try:
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})

        def start_sync_yida():
            SyncBankLtv.main()

        threading.Thread(target=start_sync_yida).start()
        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
