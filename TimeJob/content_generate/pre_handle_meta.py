import datetime
import json
import uuid

import requests

from lib.Common.utils import clean_none
from service.ReportService import add_error_log, add_fatal_log
from service.ResearchService import get_research_need_handle_meta, set_research_meta_data, add_research_attribute


def pre_handle_meta():
    try:
        start_time = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        
        research = get_research_need_handle_meta(start_time)
        for i in research:
            try:
                url = "http://task.fargoinsight.com/v1/workflows/run"
                payload = {
                    "inputs": {"report_code": f"{i['uuid']}"},
                    "response_mode": "blocking",
                    "user": "system"
                }
                headers = {
                    "Authorization": "Bearer app-mmsTNcWVl2lNziFttelyTRAQ",
                    "Content-Type": "application/json",
                    "content-type": "application/json"
                }
                response = requests.request("POST", url, json=payload, headers=headers, timeout=(300.0, 600.0))
                if response.status_code != 200:
                    set_research_meta_data(research_id=i['uuid'],
                                           author='',
                                           region='',
                                           company_cn='[]',
                                           company_en='[]',
                                           stock_ticker='[]',
                                           industry='[]',
                                           sector='[]',
                                           asset_class='',
                                           tag='[]'
                                           )
                    add_error_log(f"call meta data handle api fail{response.text}")
                    continue
                output = json.loads(response.text)['data']['outputs']['output']
                result = json.loads(output)
                if add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=i['uuid'], attribute="resolve_meta",
                                          value=json.dumps(result),
                                          create_time=datetime.datetime.now()):
                    add_research_attribute(p_key=f"{uuid.uuid1()}", research_id=i['uuid'], attribute="meta_summary",
                                           value=result.get('summary', ''),
                                           create_time=datetime.datetime.now())
                    result = clean_none(result)
                    set_research_meta_data(research_id=i['uuid'],
                                           author=result.get('author', ''),
                                           region=result.get('region', ''),
                                           company_cn=str(result.get('company_cn', '[]')),
                                           company_en=str(result.get('company_en', '[]')),
                                           stock_ticker=str(result.get('stock_ticker', '[]')),
                                           industry=str(result.get('industry', '[]')),
                                           sector=str(result.get('sector', '[]')),
                                           asset_class=result.get('asset_class', ''),
                                           tag=str(result.get('tag', '[]'))
                                           )
            except Exception as e:
                set_research_meta_data(research_id=i['uuid'],
                                       author='',
                                       region='',
                                       company_cn='[]',
                                       company_en='[]',
                                       stock_ticker='[]',
                                       industry='[]',
                                       sector='[]',
                                       asset_class='',
                                       tag='[]'
                                       )
                print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
        add_fatal_log(message=f"handle meta,fatal:{str(e)}")


if __name__ == '__main__':
    pre_handle_meta()
