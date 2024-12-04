import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict, query


def get_ticker_all():
    sql = f"select * from TB_Script_CompanyNameCode"
    data = query_dict(sql)
    return data


def get_ticker_by_source_symbol(symbol, source):
    sql = f"select * from TB_Script_CompanyNameCode where symbol=%s and source=%s"
    data = query_dict(sql, (symbol, source))
    return data


def get_ticker_by_keyword(keyword):
    sql = f'select * from TB_Script_CompanyNameCode where symbol like "%%"%s"%%"'
    return query_dict(sql, (keyword,))


def get_company_by_name(name):
    sql = f'select * from TB_Company where en_name like "%%"%s"%%"'
    data = query_dict(sql, (name,))
    return data


def add_ticker_name(platform, company_code, company_id, symbol, creator, company_name):
    sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
    return execute(sql, (
        f"{uuid.uuid1()}", platform, company_id, company_code, company_name, symbol, datetime.datetime.now()))


def add_ticker(platform, company_code, company_name, symbol, creator):
    company_uuid = uuid.uuid1()
    sql = f"insert into TB_Company(id,cn_name,en_name,creator,create_time) values (%s,%s,%s,%s,%s)"
    res = execute(sql, (f"{company_uuid}", company_name, company_name, creator, datetime.datetime.now()))
    if res:
        sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
        return execute(sql, (
            f"{uuid.uuid1()}", platform, company_uuid, company_code, company_name, symbol, datetime.datetime.now()))
    else:
        return None
