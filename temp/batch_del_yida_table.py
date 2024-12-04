

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
    basic_bank_form_uuid, basic_app_type, basic_system_token, ltv_system_token, ltv_app_type


class BatchDel:
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
    def create_del_client() -> dingtalkyida_1_0Client:
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
    def main(
            args: List[str],
    ) -> None:
        try:
            client = BatchDel.create_client()
            auth_client = BatchDel.create_auth_client()
            get_access_token_request = dingtalkoauth_2__1__0_models.GetAccessTokenRequest(
                app_key=app_key,
                app_secret=app_secret
            )
            access_token = auth_client.get_access_token(get_access_token_request).body.access_token
            search_form_datas_headers = dingtalkyida__1__0_models.SearchFormDatasHeaders()
            search_form_datas_headers.x_acs_dingtalk_access_token = f'{access_token}'
            res_dict = {}

            remove_client = BatchDel.create_client()
            batch_removal_by_form_instance_id_list_headers = dingtalkyida__1__0_models.BatchRemovalByFormInstanceIdListHeaders()
            batch_removal_by_form_instance_id_list_headers.x_acs_dingtalk_access_token = f'{access_token}'

            for i in range(1, 190):
                search_form_datas_request = dingtalkyida__1__0_models.SearchFormDatasRequest(
                    system_token=ltv_system_token,
                    page_size=100,
                    form_uuid='FORM-3347911F5E10458C91B863D209DCB175642R',
                    current_page=i,
                    user_id=user_id,
                    app_type=ltv_app_type
                )
                data = client.search_form_datas_with_options(search_form_datas_request, search_form_datas_headers,
                                                             util_models.RuntimeOptions())
                print(data.body.total_count)
                form_instance_id_list = []
                for h in data.body.data:
                    form_instance_id_list.append(h.form_instance_id)
                batch_removal_by_form_instance_id_list_request = dingtalkyida__1__0_models.BatchRemovalByFormInstanceIdListRequest(
                    form_uuid='FORM-3347911F5E10458C91B863D209DCB175642R',
                    app_type=ltv_app_type,
                    asynchronous_execution=False,
                    system_token=ltv_system_token,
                    form_instance_id_list=form_instance_id_list,
                    user_id=user_id,
                    execute_expression=False
                )
                try:
                    remove_client.batch_removal_by_form_instance_id_list_with_options(
                        batch_removal_by_form_instance_id_list_request,
                        batch_removal_by_form_instance_id_list_headers, util_models.RuntimeOptions())
                except Exception as err:
                    print(str(err))


        except Exception as err:
            print(str(err))


if __name__ == '__main__':
    BatchDel.main(sys.argv[1:])
