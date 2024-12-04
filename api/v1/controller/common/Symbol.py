import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.ReportService import add_error_log
from api.v1.service.SymbolService import get_all_symbol_simple, get_symbol_search

LOG_TAG = "sys symbol api"
symbol_controller = Blueprint('symbol_controller', __name__)


@symbol_controller.route('/v1/symbol', methods=['POST'])
def symbol_search():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        symbol = req_data.get('symbol')
        ticker = req_data.get('ticker')
        isin = req_data.get('isin')
        ric = req_data.get('ric')
        bbg_code = req_data.get('bbg_code')
        name = req_data.get('name')
        if symbol or ticker or isin or ric or bbg_code or name:
            res['data'] = get_symbol_search(symbol=symbol, ticker=ticker, isin=isin, ric=ric, bbg_code=bbg_code,
                                            en_name=name,
                                            cn_name=name)
        else:
            res['data'] = get_all_symbol_simple()

        res['status'] = True
    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
