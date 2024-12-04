import json
from datetime import datetime, timedelta

import requests

from service.InsightService import set_sync_node_time
from service.ReportService import add_error_log, add_fatal_log
from service.ResearchService import get_fargo_insight_research_not_sync
from service.UUIDCheckService import get_uuid_check, add_uuid_check

FARGO_INSIGHT_KEY = '2e5bbe02-1e66-472e-937c-8d2ded7b4314'
ARTICLE_TABLE = 'native-table-BIQ3ClsZkKo2etsQEtkX'
IB_SOURCE = ['ubs', 'ms', 'gs', 'cicc', 'jp']
TICKER_FILTER = ['AMZN', 'BX', 'WFC', '0960', '2618', 'CCL', 'BA', 'SQ', '6618', 'MGM', '0853', '9922', '0909', '6862',
                 '3333', '6098', '1113', '9698', 'DIS', 'ORCL', 'CRM', 'AI', 'TSM', '2018', '0001', 'BEKE', 'XPEV',
                 '0836', 'SPOT', '2388', '0027', '0066', '0293', '1109', 'BABA', 'NIO', '0688', '6993', 'LULU', '0002',
                 'MRVL', '6060', '0011', 'TRIP', '0006', '0003', 'JPM', '6185', 'TTD', '0762', 'SNPS', 'LRCX', '2202',
                 '6030', 'CME', '6865', '1288', '2238', 'MARA', 'WYNN', '0763', 'CMCSA', '2328', '1398', '2628', '0916',
                 '0728', '3988', '0694', '1919', '8083', '1193', '0636', '2689', '0017', '2269', 'UAL', '2333', '0175',
                 '0016', '2502', '0384', 'AIR', '9633', '0823', '0941', '2171', '1368', '2359', '2331', '0857', '2899',
                 '1378', '0981', '0386', '0883', '2020', '0388', '1458', '3968', '0005', '1810', 'UNH', 'MET', 'OXY',
                 'MMM', 'VIPS', 'JD', 'NTES', 'BILI', 'SPT', 'YSG', 'BAC', 'NVO', 'PDD', '9020', 'AZN', '005930', 'TME',
                 'RTN', 'GSK', 'QD', 'TAL', 'BARC', 'EDU', 'HUYA', 'V', 'SNOW', '8031', 'X', '8697', 'U', '8410', 'TEF',
                 '8002', '8001', '8053', '8316', '6501', 'RBLX', 'MS', '7267', 'PLTR', 'MCD', 'SNAP', '8411', '6954',
                 '6702', 'NOW', '8604', '7201', 'YMM', 'GS', '6902', 'LVS', 'K', 'MA', '1339', 'F', '9984', '1138',
                 '7203', 'GM', '9201', 'DAL', 'CVX', 'DASH', '6886', '2313', '1658', '0939', 'C', '1988', 'KO', 'AA',
                 '3033', '9626', 'CAT', 'AMC', '1772', 'PFE', 'T', '1801', '1336', '6066', 'SHOP', '2601', '1928',
                 '9992', 'NKE', '2822', 'FCX', 'SE', '2013', '1519', 'WMT', 'UBER', '9868', '2382', '6969', 'ON',
                 'MRNA', 'WBD', 'PANW', 'MDB', 'PAH3', 'RIVN', 'SMCI', 'COIN', 'UBSG', 'ALV', 'ARM', 'BAYN', 'VOW3',
                 'IQ', 'MOMO', 'IMAB', 'SAP', 'KC', 'SIE', 'DTE', 'ASML', 'AVGO', 'SBUX', 'Z74', 'TXN', 'ROKU', 'HOOD',
                 'F34', 'GDS', 'C6L', 'O39', 'NVAX', 'BN4', 'D05', 'AJBU', 'S68', 'GILD', 'LYFT', 'FFIE', 'N2IU',
                 'ILMN', 'A17U', 'ETSY', 'ISRG', 'IBKR', 'D01', 'STM', 'GOLD', 'DBX', 'CRWD', 'BNTX', 'TMO', 'C38U',
                 'NCLH', 'BYND', 'CZR', 'AMAT', 'AAL', 'CHPT', 'COST', 'TSN', 'TWLO', 'TEAM', 'ZM', 'PYPL', 'KMB',
                 'LLY', 'TSLA', 'INTC', 'QCOM', 'AXP', 'NVDA', 'NET', 'BLK', 'ALB', 'AMD', 'MAR', 'MU', 'CVS', 'MSFT',
                 'DDOG', 'FDX', 'ZS', 'GE', 'ABNB', 'EL', 'VZ', 'ADBE', 'AAPL', 'COP', 'BIDU', 'META', 'NFLX', 'LI',
                 'FUTU', 'DELL', 'GOOGL', 'TM']
TICKER_FILTER = set(TICKER_FILTER)


def sync_research():
    files = get_fargo_insight_research_not_sync(create_time=(datetime.now() - timedelta(hours=1)))
    temp = []
    for file in files:
        try:
            result = False
            stock_ticker = set(eval(f"{file['stock_ticker']}"))
            if file['file_type'] == 'zip':
                continue
            if file['file_type'] == 'xls':
                continue
            if not file['stock_ticker']:
                continue
            if len(stock_ticker) == 0:
                continue
            if not file['download_status']:
                continue
            for i in stock_ticker:
                if i in TICKER_FILTER:
                    result = True
            if not result:
                continue
            if (stock_ticker & TICKER_FILTER) == 0:
                continue
            if file['source'] in IB_SOURCE and file['file_type'] == 'pdf' and file['parse_status'] == 'parse_ok':
                temp.append(file)
                continue
            if file['source'] in IB_SOURCE and not file['file_type'] == 'pdf':
                temp.append(file)
                continue
            if file['source'] == 'quartr' and file['business_type'] == 'slides':
                temp.append(file)
                continue
            if file['source'] == 'quartr' and file['business_type'] == 'audio' and file['parse_status'] == 'parse_ok':
                temp.append(file)
                continue
            if file['source'] == 'quartr' and file['business_type'] == 'report' and file['parse_status'] == 'parse_ok':
                temp.append(file)
            if file['source'] == 'sa' and file['file_type'] == 'html':
                temp.append(file)
            if file['source'] == 'fargo' and file['file_type'] == 'html':
                temp.append(file)
        except Exception as e:
            add_error_log(message=f"fail:{str(e)}")
    files = temp
    for file in files:
        try:

            source_type = "COM"
            title = f"{file['title']}"
            if file['source'] in ['ms', 'ubs', 'gs', 'jp', 'cicc']:
                source_type = "IB"
            if file['source'] == 'sa':
                source_type = "IA"
            if file['source'] == 'fargo':
                source_type = "IA"
            if file['source'] == 'quartr':
                source_type = 'COM'
            check = get_uuid_check(file['uuid'])
            if len(check) > 0:
                continue
            r = requests.post(
                "https://api.glideapp.io/api/function/mutateTables",
                headers={"Authorization": f"Bearer {FARGO_INSIGHT_KEY}"},
                json={
                    "appID": "uNOgjdbeolykCXHBMvi0",
                    "mutations": [
                        {
                            "kind": "add-row-to-table",
                            "tableName": "native-table-05dcfafe-2e38-4006-8a0e-d213acb33867",
                            "columnValues": {
                                "Name": f"{file['uuid']}",
                                "nv9r2": f"{file['source']}",
                                "Y5sDG": f"{file['publish_time']}",
                                "fRaMv": f"{file['create_time']}",
                                "yVixt": f"{title}",
                                "3MHiJ": f"{file['tags']}",
                                "iuCqk": f"{file['file_type']}",
                                "Mr2Br": f"{file['lang']}",
                                "SI2Ed": f"{file['local_path']}",
                                "UHJEZ": f"{file['status']}",
                                "Kp0im": f"{file['download_status']}",
                                "67P3z": f"{file['meta_data']}",
                                "C5Ge8": f"{file['parse_status']}",
                                "zjm0T": f"{file['embedding_status']}",
                                "yHG3w": f"{file['author']}",
                                "GWT6F": f"{file['source_url']}",
                                "gexS5": f"{file['preprocess_status']}",
                                "weTrs": source_type,
                                "mvzGD": f"{file['event_id']}",
                                "sbK6h": f"{file['business_type']}",
                                "I7usW": f"{datetime.now()}",
                            }
                        }
                    ]
                }, timeout=(120.0, 300.0)
            )
            if r.status_code != 200:
                add_fatal_log(f"fail")
            else:
                if add_uuid_check(id=file['uuid']):
                    set_sync_node_time(node_time=file['create_time'], update_time=datetime.now(),
                                       task_name='sync_research')
        except Exception as e:
            print(str(e))
            add_error_log(message=f"fail:{str(e)}")


if __name__ == '__main__':
    sync_research()
