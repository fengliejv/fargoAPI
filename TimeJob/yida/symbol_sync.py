

import math
import sys

from typing import List

from alibabacloud_dingtalk.yida_1_0.client import Client as dingtalkyida_1_0Client
from alibabacloud_dingtalk.yida_1_0 import models as dingtalkyida__1__0_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_dingtalk.oauth2_1_0.client import Client as dingtalkoauth2_1_0Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dingtalk.oauth2_1_0 import models as dingtalkoauth_2__1__0_models

from TimeJob.yida.yida_config import app_key, app_secret, user_id, basic_app_type, basic_system_token, \
    basic_symbol_form_uuid
from service.SymbolService import get_all_symbol, add_symbol, update_symbol


class SyncSymbol:
    def __init__(self):
        pass

    @staticmethod
    def create_client() -> dingtalkyida_1_0Client:
        """
        使用 Token 初始化账号Client
        @return: Client
        @throws Exception
        """
        config = open_api_models.Config()
        config.protocol = 'https'
        config.region_id = 'central'
        return dingtalkyida_1_0Client(config)

    @staticmethod
    def create_auth_client() -> dingtalkoauth2_1_0Client:
        """
        使用 Token 初始化账号Client
        @return: Client
        @throws Exception
        """
        config = open_api_models.Config()
        config.protocol = 'https'
        config.region_id = 'central'
        return dingtalkoauth2_1_0Client(config)

    @staticmethod
    def main(
            args: List[str],
    ) -> None:
        try:
            client = SyncSymbol.create_client()
            auth_client = SyncSymbol.create_auth_client()
            get_access_token_request = dingtalkoauth_2__1__0_models.GetAccessTokenRequest(
                app_key=app_key,
                app_secret=app_secret
            )
            access_token = auth_client.get_access_token(get_access_token_request).body.access_token
            search_form_datas_headers = dingtalkyida__1__0_models.SearchFormDatasHeaders()
            search_form_datas_headers.x_acs_dingtalk_access_token = f'{access_token}'
            search_form_datas_request = dingtalkyida__1__0_models.SearchFormDatasRequest(
                system_token=basic_system_token,
                page_size=100,
                form_uuid=basic_symbol_form_uuid,
                current_page=1,
                user_id=user_id,
                app_type=basic_app_type
            )
            data = client.search_form_datas_with_options(search_form_datas_request, search_form_datas_headers,
                                                         util_models.RuntimeOptions())
            if data.body.total_count == 0:
                return
            res_dic = set()
            res_dic.add(data)
            symbol = get_all_symbol()
            symbol_dict = {}
            for i in symbol:
                symbol_dict[i['symbol']] = i
            cycle_count = math.ceil(data.body.total_count / 100) + 1

            for i in range(2, cycle_count):
                search_form_datas_request = dingtalkyida__1__0_models.SearchFormDatasRequest(
                    system_token=basic_system_token,
                    page_size=100,
                    form_uuid=basic_symbol_form_uuid,
                    current_page=i,
                    user_id=user_id,
                    app_type=basic_app_type
                )
                data = client.search_form_datas_with_options(search_form_datas_request, search_form_datas_headers,
                                                             util_models.RuntimeOptions())
                res_dic.add(data)
            for res in res_dic:
                for line in res.body.data:
                    
                    try:
                        req_data = line.form_data
                        symbol_code = req_data['textField_w6a6smd']
                        isin = req_data['textField_nzp7g8v'] if 'textField_nzp7g8v' in req_data else ''
                        en_name = req_data['textField_1cpdj0f'] if 'textField_1cpdj0f' in req_data else ''
                        cn_name = req_data['textField_y89voyc'] if 'textField_y89voyc' in req_data else ''
                        country_code = req_data['textField_wpsjtxj'] if 'textField_wpsjtxj' in req_data else ''
                        country = req_data['textField_55cq79e'] if 'textField_55cq79e' in req_data else ''
                        bbg_code = req_data['textField_mbvliyx'] if 'textField_mbvliyx' in req_data else ''
                        ticker = req_data['textField_24r845j'] if 'textField_24r845j' in req_data else ''
                        ric = req_data['textField_evmp19p'] if 'textField_evmp19p' in req_data else ''
                        exchanger = req_data['textField_lx45mtkq'] if 'textField_lx45mtkq' in req_data else ''
                        currency = req_data['textField_lxo0puma'] if 'textField_lxo0puma' in req_data else ''
                        sf_code = req_data['textField_lxo10w5b'] if 'textField_lxo10w5b' in req_data else ''
                        gs_code = req_data['textField_lxo10w5d'] if 'textField_lxo10w5d' in req_data else ''
                        quartr_code = req_data['textField_lxo10w5f'] if 'textField_lxo10w5f' in req_data else ''
                        sa_code = req_data['textField_lxo10w5h'] if 'textField_lxo10w5h' in req_data else ''
                        markets_code = req_data['textField_lxo10w5i'] if 'textField_lxo10w5i' in req_data else ''
                        is_ETF = req_data['radioField_lxo3huta'] if 'radioField_lxo3huta' in req_data else ''
                        is_COM = req_data['radioField_lxo3hutc'] if 'radioField_lxo3hutc' in req_data else ''
                        if symbol_code not in symbol_dict:
                            
                            add_symbol(symbol=symbol_code, isin=isin, cn_name=cn_name, en_name=en_name,
                                       country_code=country_code, country=country, bbg_code=bbg_code, ticker=ticker,
                                       ric=ric, exchanger=exchanger, currency=currency,
                                       sf_code=sf_code, gs_code=gs_code, quartr_code=quartr_code, sa_code=sa_code,
                                       markets_code=markets_code, is_ETF=is_ETF, is_COM=is_COM)
                        else:
                            
                            if {isin, en_name, cn_name, country_code, country, bbg_code, ticker, ric, exchanger,
                                currency, sf_code, gs_code, quartr_code, sa_code, markets_code, is_ETF, is_COM} == {
                                symbol_dict[symbol_code]['isin'], symbol_dict[symbol_code]['en_name'],
                                symbol_dict[symbol_code]['cn_name'], symbol_dict[symbol_code]['country_code'],
                                symbol_dict[symbol_code]['country'], symbol_dict[symbol_code]['bbg_code'],
                                symbol_dict[symbol_code]['ticker'], symbol_dict[symbol_code]['ric'],
                                symbol_dict[symbol_code]['exchanger'], symbol_dict[symbol_code]['currency'],
                                symbol_dict[symbol_code]['sf_code'], symbol_dict[symbol_code]['gs_code'],
                                symbol_dict[symbol_code]['quartr_code'], symbol_dict[symbol_code]['sa_code'],
                                symbol_dict[symbol_code]['markets_code'], symbol_dict[symbol_code]['is_ETF']
                                , symbol_dict[symbol_code]['is_COM'],
                            }:
                                continue
                            print(symbol_code)
                            update_symbol(symbol_id=symbol_dict[symbol_code]['uuid'], isin=isin, cn_name=cn_name,
                                          en_name=en_name, country_code=country_code, country=country,
                                          bbg_code=bbg_code, ticker=ticker,
                                          ric=ric, exchanger=exchanger, currency=currency, sf_code=sf_code,
                                          gs_code=gs_code, quartr_code=quartr_code, sa_code=sa_code,
                                          markets_code=markets_code, is_ETF=is_ETF, is_COM=is_COM)
                    except Exception as err:
                        print(str(err))
        except Exception as err:
            print(str(err))


if __name__ == '__main__':
    SyncSymbol.main(sys.argv[1:])
