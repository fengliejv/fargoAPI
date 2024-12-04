

import math
import sys

from typing import List

from alibabacloud_dingtalk.yida_1_0.client import Client as dingtalkyida_1_0Client
from alibabacloud_dingtalk.yida_1_0 import models as dingtalkyida__1__0_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_dingtalk.oauth2_1_0.client import Client as dingtalkoauth2_1_0Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dingtalk.oauth2_1_0 import models as dingtalkoauth_2__1__0_models

from TimeJob.yida.yida_config import app_key, app_secret, ltv_form_uuid, user_id, ltv_app_type, \
    ltv_system_token
from service.BankService import get_all_bank
from service.SymbolService import get_all_symbol
from service.BankLtvService import add_bank_ltv, get_all_bank_ltv, update_bank_ltv


class SyncBankLtv:
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
            client = SyncBankLtv.create_client()
            auth_client = SyncBankLtv.create_auth_client()
            get_access_token_request = dingtalkoauth_2__1__0_models.GetAccessTokenRequest(
                app_key=app_key,
                app_secret=app_secret
            )
            access_token = auth_client.get_access_token(get_access_token_request).body.access_token
            search_form_datas_headers = dingtalkyida__1__0_models.SearchFormDatasHeaders()
            search_form_datas_headers.x_acs_dingtalk_access_token = f'{access_token}'
            search_form_datas_request = dingtalkyida__1__0_models.SearchFormDatasRequest(
                system_token=ltv_system_token,
                page_size=100,
                form_uuid=ltv_form_uuid,
                current_page=1,
                user_id=user_id,
                app_type=ltv_app_type
            )
            data = client.search_form_datas_with_options(search_form_datas_request, search_form_datas_headers,
                                                         util_models.RuntimeOptions())
            if data.body.total_count == 0:
                return
            res_dic = set()
            res_dic.add(data)
            ltv = get_all_bank_ltv()
            ltv_dict = {}
            for i in ltv:
                ltv_dict[f"{i['bank_id']}{i['symbol_id']}{i['quote_time']}"] = i
            cycle_count = math.ceil(data.body.total_count / 100) + 1

            bank = get_all_bank()
            bank_dict = {}
            for i in bank:
                bank_dict[i['bank_code']] = i

            symbols = get_all_symbol()
            symbol_dict = {}
            for i in symbols:
                if i['isin']:
                    symbol_dict[i['isin']] = i

            for i in range(1, cycle_count):
                search_form_datas_request = dingtalkyida__1__0_models.SearchFormDatasRequest(
                    system_token=ltv_system_token,
                    page_size=100,
                    form_uuid=ltv_form_uuid,
                    current_page=i,
                    user_id=user_id,
                    app_type=ltv_app_type
                )
                data = client.search_form_datas_with_options(search_form_datas_request, search_form_datas_headers,
                                                             util_models.RuntimeOptions())
                res_dic.add(data)
            for res in res_dic:
                for line in res.body.data:

                    try:
                        req_data = line.form_data
                        bank = req_data['selectField_lwwxfl6s']
                        isin = req_data['textField_lwwwiz0r']
                        bank_ltv = req_data['numberField_lwwwiz0y']
                        quote_time = req_data['dateField_lwwwrcdc']


                        if bank not in bank_dict:

                            continue
                        if isin not in symbol_dict:

                            continue
                        dict_key = f"{bank_dict[bank]['uuid']}{symbol_dict[isin]['uuid']}{quote_time}"
                        if dict_key in ltv_dict:

                            if bank_ltv == ltv_dict[dict_key]['ltv']:
                                continue
                            else:
                                update_bank_ltv(ltv_id=ltv_dict[dict_key]['uuid'], ltv=bank_ltv)
                        else:

                            add_bank_ltv(bank_id=bank_dict[bank]['uuid'], symbol_id=symbol_dict[isin]['uuid'],
                                         ltv=bank_ltv,
                                         quote_time=quote_time)
                    except Exception as err:
                        print(str(err))
        except Exception as err:
            print(str(err))


if __name__ == '__main__':
    SyncBankLtv.main(sys.argv[1:])
