import datetime
import json

from lib.Common.mysqlsingle import query_dict
from service.ReportService import add_fatal_log, add_tb_file_basic_record, add_info_log, get_all_company_symbol, \
    check_repeat_report
from config import Config


def run(source, path):
    SOURCE = source
    PATH = path
    print("run")
    company_dict = get_all_company_symbol(type=SOURCE)
    com_dict = dict()
    for company in company_dict:
        com_dict.setdefault(company["en_name"].replace(".", "").replace(" ", "").replace("&", ""), company["symbol"])
    
    
    
    
    
    
    
    
    
    sql = """
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY id) AS rn
        FROM TB_File
        WHERE
          publish_time > '2024-01-01 00:00:00'
          AND publish_time < '2024-07-01 00:00:00'
          AND file_path like %s and type=%s
    ) AS b 
    WHERE rn = 1 
    AND file_path NOT IN (
       SELECT DISTINCT local_save_path
       FROM TB_File_Basic
       WHERE 
         publish_time > '2024-01-01 00:00:00'
         AND publish_time < '2024-07-01 00:00:00'
    )
    """
    files = query_dict(sql, (f"%/{SOURCE}/%", 'pdf'))
    count = 0
    for file in files:
        count += 1
        print(count)
        try:
            article_id = None
            if SOURCE == 'gs':
                article_id = json.loads(file['attribute'])['id']
            elif SOURCE == 'ubs':
                article_id = json.loads(file['attribute'])['wireId']
            elif SOURCE == 'ms':
                article_id = json.loads(file['attribute'])['id']
            symbol = file['symbol']
            if not symbol:
                continue
            check_repeat = check_repeat_report(source=SOURCE, article_id=article_id, symbol=symbol)
            if len(check_repeat) > 0:
                print("replate")
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
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')
            add_fatal_log(message=f"定时解析{SOURCE}报告异常,TB_File_ID：{file['id']},报错:{str(e)}")


if __name__ == '__main__':
    
    
    run(source='ms', path="/home/ibagents/files/ms")
