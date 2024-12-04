import datetime
import json

from lib.Common.utils import generate_hash
from service.ReportService import add_fatal_log, \
    get_all_tb_file_by_publish_time_and_source, add_tb_file_basic_record, add_info_log, get_all_tb_file_basic_by_source, \
    get_all_company_symbol, check_repeat_report, get_need_format_file, check_repeat_file, add_tb_file_basic, \
    get_need_format_file_recently
from config import Config

SOURCE = "ms"
PATH = f"/home/ibagents/files/{SOURCE}"


def parse_ms_file_to_file_basic():
    print("parse_ms_file_to_file_basic")
    company_dict = get_all_company_symbol(type=SOURCE)
    com_dict = dict()
    for company in company_dict:
        com_dict.setdefault(company["en_name"].replace(".", "").replace(" ", "").replace("&", ""), company["symbol"])
    files = get_need_format_file_recently(source=SOURCE, type='pdf')
    
    for file in files:
        try:
            last_index = file['file_path'].rfind("/")
            second_index = file['file_path'].rfind("/", 0, last_index)
            ticker = file['file_path'][second_index + 1:last_index]
            if ticker == "sub":
                symbol = None
                file_attribute = json.loads(file['attribute'])
                if "reports" in file_attribute:
                    article_id = file_attribute['reports'][0]['id']
                    attribute = file_attribute['userEvent']['events'][0]['att']['ed'][0]['name']
                else:
                    article_id = file_attribute['id']
                    attribute = file_attribute['subject_name']
                check_repeat = check_repeat_file(source=SOURCE, article_id=article_id)
                if len(check_repeat) > 0 or not file['attribute']:
                    continue
                res = add_tb_file_basic(
                    article_id=article_id, file_id=file['id'],
                    source=SOURCE, title=file['title'], lang="en-US", original_url=file['source'],
                    local_save_path=file['file_path'], publish_time=file['publish_time'], creator='system',
                    create_time=datetime.datetime.now(), symbol=symbol,
                    attribute=attribute)
            else:
                file_attribute = json.loads(file['attribute'])
                if not file_attribute == {}:
                    article_id = file_attribute['id']
                    symbol = com_dict.get(ticker.replace(".", "").replace(" ", "").replace("&", ""))
                    check_repeat = check_repeat_report(source=SOURCE, article_id=article_id, symbol=symbol)
                    if len(check_repeat) > 0:
                        continue
                    res = add_tb_file_basic_record(
                        article_id=article_id, file_id=file['id'],
                        source=SOURCE, title=file['title'], lang="en-US", original_url=file['source'],
                        local_save_path=file['file_path'], publish_time=file['publish_time'], creator='system',
                        create_time=datetime.datetime.now(), symbol=symbol)
                else:
                    symbol = None
                    article_id = generate_hash(file_path=file['file_path'])
                    check_repeat = check_repeat_report(source=SOURCE, article_id=article_id, symbol=symbol)
                    if len(check_repeat) > 0:
                        continue
                    attribute = "private"
                    res = add_tb_file_basic(
                        article_id=article_id, file_id=file['id'],
                        source=SOURCE, title=file['title'], lang="en-US", original_url=file['source'],
                        local_save_path=file['file_path'], publish_time=file['publish_time'], creator='system',
                        create_time=datetime.datetime.now(), symbol=symbol, attribute=attribute)

            if res:
                print(f"定时解析{SOURCE}报告成功,TB_File_ID：{file['id']}")
                add_info_log(message=f"定时解析{SOURCE}:tb_file_basic成功,TB_File_ID：{file['id']}")

        except Exception as e:
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
            add_fatal_log(message=f"定时解析{SOURCE}报告异常,TB_File_ID：{file['id']},报错:{str(e)}")


if __name__ == '__main__':
    parse_ms_file_to_file_basic()
