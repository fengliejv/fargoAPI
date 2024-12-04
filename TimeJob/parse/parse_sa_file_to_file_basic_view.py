import datetime
import io
import json
import re

from service.ReportService import add_fatal_log, add_info_log, get_file_basic_not_parse_uuid_by_source, \
    add_tb_file_basic_view_record

SOURCE = "sa"
PATH = f"/home/ibagents/files/{SOURCE}"


def parse_sa_file_to_file_basic_view():
    all_file_basic = get_file_basic_not_parse_uuid_by_source(source=SOURCE)
    for file in all_file_basic:
        try:
            f = io.open(file['local_save_path'], mode='r', encoding='utf-8')
            content = f.read()
            f.close()
            rating = re.findall("{([^{}]*sentiment\",\"attributes\".*?)}", content, flags=re.S)
            if not len(rating) > 0:
                continue
            rating = str(rating[0])
            rating = rating[rating.find("{"):len(rating)] + "}"
            rating = json.loads(rating)
            if len(rating) > 0:
                rating = rating['type']
            else:
                continue
            last_index = file['local_save_path'].rfind("/")
            second_index = file['local_save_path'].rfind("/", 0, last_index)
            ticker = file['local_save_path'][second_index + 1:last_index]
            res = add_tb_file_basic_view_record(file_basic_id=file['uuid'], ticker=ticker, sn=1, rating=rating)
            if res:
                print(f"定时解析{SOURCE}观点成功,TB_File_Basic_id：{file['uuid']}")
                add_info_log(message=f"定时解析{SOURCE}观点成功,TB_File_Basic_id：{file['uuid']}")

        except Exception as e:
            add_fatal_log(message=f"定时解析{SOURCE}观点成功,TB_File_Basic_id：{file['uuid']},报错:{str(e)}")


if __name__ == '__main__':
    parse_sa_file_to_file_basic_view()
