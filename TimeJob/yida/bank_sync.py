

import math
import sys

from typing import List

from alibabacloud_dingtalk.yida_1_0.client import Client as dingtalkyida_1_0Client
from alibabacloud_dingtalk.yida_1_0 import models as dingtalkyida__1__0_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_dingtalk.oauth2_1_0.client import Client as dingtalkoauth2_1_0Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dingtalk.oauth2_1_0 import models as dingtalkoauth_2__1__0_models

from TimeJob.yida.yida_config import app_key, app_secret, user_id, \
    basic_bank_form_uuid, basic_app_type, basic_system_token
from service.BankService import get_all_bank, add_bank, update_bank


class SyncBank:
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
            client = SyncBank.create_client()
            auth_client = SyncBank.create_auth_client()
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
                form_uuid=basic_bank_form_uuid,
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
            bank = get_all_bank()
            bank_dict = {}
            for i in bank:
                bank_dict[i['bank_code']] = i
            cycle_count = math.ceil(data.body.total_count / 100) + 1

            for i in range(2, cycle_count):
                search_form_datas_request = dingtalkyida__1__0_models.SearchFormDatasRequest(
                    system_token=basic_system_token,
                    page_size=100,
                    form_uuid=basic_bank_form_uuid,
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

                        bank_code = req_data['textField_lx33bqu7']
                        booking_centre = req_data['selectField_lx33bquc']
                        bank_name_en = req_data['textField_4h9wc1q']
                        bank_name_cn = req_data['textField_lx344je5'] if 'textField_lx344je5' in req_data else ''
                        support_feature = req_data[
                            'checkboxField_lx35x39e'] if 'checkboxField_lx35x39e' in req_data else ''
                        support_feature = ','.join(support_feature)
                        if bank_code not in bank_dict:
                            
                            add_bank(bank_code=bank_code, en_bank_name=bank_name_en, booking_centre=booking_centre,
                                     cn_bank_name=bank_name_cn, support=support_feature)
                        else:
                            
                            if {booking_centre, bank_name_cn, bank_name_en, support_feature} == {
                                bank_dict[bank_code]['booking_centre'], bank_dict[bank_code]['en_bank_name'],
                                bank_dict[bank_code]['cn_bank_name'], bank_dict[bank_code]['support']}:
                                continue
                            update_bank(bank_id=bank_dict[bank_code]['uuid'], en_bank_name=bank_name_en,
                                        booking_centre=booking_centre, cn_bank_name=bank_name_cn,
                                        support=support_feature)
                    except Exception as err:
                        print(str(err))
        except Exception as err:
            print(str(err))


if __name__ == '__main__':
    SyncBank.main(sys.argv[1:])
