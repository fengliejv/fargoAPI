import datetime
import re
import uuid

from lib.Common.mysqlsingle import query_dict, execute


def sanitize_en_name(en_name):
    
    illegal_chars = r'[.\',/\\&:()]'
    
    sanitized_en_name = re.sub(illegal_chars, '', en_name)
    return sanitized_en_name


def run():
    sql = "SELECT *  FROM TB_Symbol"
    data = query_dict(sql)
    count = 0
    for i in data:
        count += 1
        print(count)
        i['en_name'] = sanitize_en_name(i['en_name'])
        try:
            sql = f"select * from TB_Company where en_name='{i['en_name']}'"
            com = query_dict(sql)
            if len(com) == 0:
                sql = f"insert into TB_Company(id,cn_name,en_name,creator,create_time) values (%s,%s,%s,%s,%s)"
                com_id = f"{uuid.uuid1()}"
                res = execute(sql,
                              (f"{com_id}", f"{i['cn_name']}", f"{i['en_name']}", 'sys', datetime.datetime.now()))
                if not res:
                    continue

                if len(i['gs_code']) > 2:
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'gs', com_id, i['gs_code'], i['en_name'], i['symbol'],
                        datetime.datetime.now()))

                if len(i['ric']) > 2:
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'ubs', com_id, i['ric'], i['en_name'], i['symbol'], datetime.datetime.now()))
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'ms', com_id, i['ric'], i['en_name'], i['symbol'], datetime.datetime.now()))

                if len(i['quartr_code']) > 2:
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'quartr', com_id, i['quartr_code'], i['en_name'], i['symbol'],
                        datetime.datetime.now()))

            if len(com) > 0:
                print("same")
                com_id = com[0]['id']
                if len(i['gs_code']) > 2:
                    sql = 'delete from TB_Script_CompanyNameCode where platform=%s and company_id=%s and company_code=%s and company_name=%s and symbol=%s'
                    execute(sql, ('gs', com_id, i['gs_code'], i['en_name'], i['symbol']))
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'gs', com_id, i['gs_code'], i['en_name'], i['symbol'],
                        datetime.datetime.now()))

                if len(i['ric']) > 2:
                    sql = 'delete from TB_Script_CompanyNameCode where platform=%s and company_id=%s and company_code=%s and company_name=%s and symbol=%s'
                    execute(sql, ('ubs', com_id, i['ric'], i['en_name'], i['symbol']))
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'ubs', com_id, i['ric'], i['en_name'], i['symbol'], datetime.datetime.now()))
                    sql = 'delete from TB_Script_CompanyNameCode where platform=%s and company_id=%s and company_code=%s and company_name=%s and symbol=%s'
                    execute(sql, ('ms', com_id, i['ric'], i['en_name'], i['symbol']))
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'ms', com_id, i['ric'], i['en_name'], i['symbol'], datetime.datetime.now()))

                if len(i['quartr_code']) > 2:
                    sql = 'delete from TB_Script_CompanyNameCode where platform=%s and company_id=%s and company_code=%s and company_name=%s and symbol=%s'
                    execute(sql, ('quartr', com_id, i['ric'], i['en_name'], i['symbol']))
                    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
                    execute(sql, (
                        f"{uuid.uuid1()}", 'quartr', com_id, i['quartr_code'], i['en_name'], i['symbol'],
                        datetime.datetime.now()))
        except Exception as e:
            print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


def sync_company():
    sql = "SELECT distinct en_name,cn_name FROM TB_Symbol"
    symbol = query_dict(sql)
    symbol_dict = {}
    sql = "SELECT distinct en_name  FROM TB_Company"
    company = query_dict(sql)
    company_dict = {}
    count = 0
    for i in symbol:
        symbol_dict[i['en_name']] = i
    for i in company:
        company_dict[i['en_name']] = i
    for i in symbol_dict:
        if i not in company_dict:
            count += 1
    print(count)


def add_company():
    sql = "SELECT distinct en_name,cn_name FROM TB_Symbol"
    symbol = query_dict(sql)
    for i in symbol:
        sql = ""


if __name__ == '__main__':
    run()
