import datetime
import json

from flask import Blueprint, request

from api.v1.lib.Jwt import check_token
from api.v1.service.ReportService import add_error_log
from api.v1.service.ResearchService import get_research

LOG_TAG = "sys research api"
research_controller = Blueprint('research_controller', __name__)


@research_controller.route('/v1/research/search', methods=['POST'])
def research_search():
    res = {
        'status': False,
        'err_msg': ""
    }
    try:
        req_data = request.get_json()
        token_dict = check_token(request.headers.get('Authorization'))
        if not token_dict[0]:
            return json.dumps({'status': False, "err_msg": "user unauthorized"})
        page_count = req_data.get('page_count')
        page_size = req_data.get('page_size')
        search = req_data.get('search')
        if not page_size or page_size > 200:
            page_size = 200
        res['data'] = []
        research = get_research(page_count=page_count, page_size=page_size, search=search)
        for i in research:
            i['download_url'] = None
            if i['source_url']:
                file_ext = i['source_url'].rpartition('.')[-1]
                i['download_url'] = f"https://files.fargoinsight.com/research/file/{i['file_id']}.{file_ext}"
            i.pop('local_path')
            res['data'].append(i)
        res['status'] = True

    except Exception as e:
        res["err_msg"] = f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}'
        add_error_log(message=res["err_msg"], source=LOG_TAG, e=e)
    return res
