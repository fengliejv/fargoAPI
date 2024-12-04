from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.asr.v20190614 import asr_client, models
import json
from flask import Blueprint, request
from api.v1.lib.Jwt import check_token
from api.v1.service.ReportService import add_error_log

LOG_TAG = "sys asr api"
asr_controller = Blueprint('asr_controller', __name__)
SecretId = 'AKIDLk5E8nDMGx1rTr21obJp7B3tdQLAOcjb'
SecretKey = 'zkG87QficM1VjhT2OZVFpAFJEzZyOckn'


@asr_controller.route('/v1/asr', methods=['POST'])
def asr():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        cred = credential.Credential(SecretId, SecretKey)
        httpProfile = HttpProfile()
        httpProfile.endpoint = "asr.tencentcloudapi.com"
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = asr_client.AsrClient(cred, "", clientProfile)
        req = models.CreateRecTaskRequest()
        params = req_data
        req.from_json_string(json.dumps(params))
        resp = client.CreateRecTask(req)
        res['data'] = json.loads(resp.to_json_string())
        res['status'] = True

    except TencentCloudSDKException as err:
        print(err)
        add_error_log(message=str(err), source=LOG_TAG)
    return res


@asr_controller.route('/v1/asr/transcript', methods=['POST'])
def asr_transcript():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        cred = credential.Credential(SecretId, SecretKey)
        httpProfile = HttpProfile()
        httpProfile.endpoint = "asr.tencentcloudapi.com"
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        client = asr_client.AsrClient(cred, "", clientProfile)
        req = models.DescribeTaskStatusRequest()
        params = req_data
        req.from_json_string(json.dumps(params))

        resp = client.DescribeTaskStatus(req)
        res['data'] = json.loads(resp.to_json_string())
        res['status'] = True

    except TencentCloudSDKException as err:
        print(err)
        add_error_log(message=str(err), source=LOG_TAG)
    return res
