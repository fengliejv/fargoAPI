import datetime
import json

from service.ReportService import add_fatal_log, \
    get_all_tb_file_by_publish_time_and_source, add_tb_file_basic_record, add_info_log, get_all_tb_file_basic_by_source, \
    get_all_company_symbol, check_repeat_report
from config import Config

SOURCE = "gs"
PATH = f"/home/ibagents/files/{SOURCE}"


def parse_gs_file_to_file_basic():
    company_dict = get_all_company_symbol(type=SOURCE)
    com_dict = dict()
    for company in company_dict:
        com_dict.setdefault(company["en_name"].replace(".", "").replace(" ", "").replace("&", ""), company["symbol"])

    all_record = get_all_tb_file_basic_by_source(source=SOURCE, key="article_id")
    if len(all_record) > 0:
        parsing_start_time = list(all_record.values())[0]['publish_time']
    else:
        parsing_start_time = Config.DATA_START_TIME

    files = get_all_tb_file_by_publish_time_and_source(publish_time="2024-01-18 00:00:00", source=SOURCE, type='pdf')

    for file in files:
        if 'ARM' not in file['file_path']:
            continue
        try:
            article_id = json.loads(file['attribute'])['id']
            if article_id in all_record:
                if all_record[article_id]['publish_time'] == file['publish_time'] and \
                        all_record[article_id][
                            'file_id'] == file['id']:

                    print("already exits")
                    continue























            last_index = file['file_path'].rfind("/")
            second_index = file['file_path'].rfind("/", 0, last_index)
            ticker = file['file_path'][second_index + 1:last_index]
            symbol = com_dict.get(ticker.replace(".", "").replace(" ", "").replace("&", ""))
            check_repeat = check_repeat_report(source=SOURCE, article_id=article_id, symbol=symbol)
            if len(check_repeat) > 0:
                continue
            res = add_tb_file_basic_record(
                article_id=article_id, file_id=file['id'],
                source=SOURCE, title=file['title'], lang="en-US", original_url=file['source'],
                local_save_path=file['file_path'], publish_time=file['publish_time'], creator='system',
                create_time=datetime.datetime.now(), symbol=symbol)
            if res:
                print(f"定时解析{SOURCE}报告成功,TB_File_ID：{file['id']}")
                add_info_log(message=f"定时解析{SOURCE}:tb_file_basic成功,TB_File_ID：{file['id']}")

        except Exception as e:
            add_fatal_log(message=f"定时解析{SOURCE}报告异常,TB_File_ID：{file['id']},报错:{str(e)}")


if __name__ == '__main__':
    parse_gs_file_to_file_basic()
