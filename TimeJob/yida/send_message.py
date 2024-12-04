

import os
import sys

from typing import List

from alibabacloud_dingtalk.aiInteraction_1_0.client import Client as dingtalkaiInteraction_1_0Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dingtalk.aiInteraction_1_0 import models as dingtalkai_interaction__1__0_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient


class Sample:
    def __init__(self):
        pass

    @staticmethod
    def create_client() -> dingtalkaiInteraction_1_0Client:
        """
        使用 Token 初始化账号Client
        @return: Client
        @throws Exception
        """
        config = open_api_models.Config()
        config.protocol = 'https'
        config.region_id = 'central'
        return dingtalkaiInteraction_1_0Client(config)

    @staticmethod
    def main(
        args: List[str],
    ) -> None:
        client = Sample.create_client()
        send_headers = dingtalkai_interaction__1__0_models.SendHeaders()
        send_headers.x_acs_dingtalk_access_token = '3af67902f3ed3ebc80a4a15e0a33cd3b'
        send_request = dingtalkai_interaction__1__0_models.SendRequest(
            open_conversation_id='',
            union_id='02441662240331132956',
            content_type='ai_card',
            content='{"header": {"title": {"type":"text","text":"这是 basic_card_schema 模式卡片"},"logo":"@lALPDfJ6V_FPDmvNAfTNAfQ"},"contents":[{"type":"text","text":"小心感冒~","id": "text_1711949486176"}]}'
        )
        try:
            client.send_with_options(send_request, send_headers, util_models.RuntimeOptions())
        except Exception as err:
            if not UtilClient.empty(err.code) and not UtilClient.empty(err.message):
                
                pass

    @staticmethod
    async def main_async(
        args: List[str],
    ) -> None:
        client = Sample.create_client()
        send_headers = dingtalkai_interaction__1__0_models.SendHeaders()
        send_headers.x_acs_dingtalk_access_token = '3af67902f3ed3ebc80a4a15e0a33cd3b'
        send_request = dingtalkai_interaction__1__0_models.SendRequest(
            open_conversation_id='',
            union_id='02441662240331132956',
            content_type='ai_card',
            content='{"header": {"title": {"type":"text","text":"这是 basic_card_schema 模式卡片"},"logo":"@lALPDfJ6V_FPDmvNAfTNAfQ"},"contents":[{"type":"text","text":"小心感冒~","id": "text_1711949486176"}]}'
        )
        try:
            await client.send_with_options_async(send_request, send_headers, util_models.RuntimeOptions())
        except Exception as err:
            if not UtilClient.empty(err.code) and not UtilClient.empty(err.message):
                
                pass


if __name__ == '__main__':
    Sample.main(sys.argv[1:])