import datetime
import time
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict


def update_bank_ltv(ltv_id, ltv):
    sql = f"update TB_Bank_ltv set ltv=%s where uuid=%s"
    return execute(sql, (ltv, ltv_id))


def get_bank_ltv_search(bank_code, symbol, start_time, end_time):
    sql = f"select * from (TB_Bank_Ltv as a inner join TB_Bank as b on a.bank_id=b.uuid) inner join TB_Symbol as c on a.symbol_id=c.uuid where bank_code=%s and symbol=%s and quote_time>=%s and quote_time<=%s"
    add_res = query_dict(sql, (bank_code, symbol, start_time, end_time))
    return add_res

def get_bank_ltv_search_symbol(symbol):
    sql = f"select * from (TB_Bank_Ltv as a inner join TB_Bank as b on a.bank_id=b.uuid) inner join TB_Symbol as c on a.symbol_id=c.uuid where symbol=%s"
    add_res = query_dict(sql, (symbol,))
    return add_res

def add_bank_ltv(bank_id, symbol_id, ltv, quote_time):
    ltv_sql = f"insert into TB_Bank_Ltv(uuid,bank_id,symbol_id,ltv,quote_time,update_time) values (%s,%s,%s,%s,%s,%s)"
    add_res = execute(ltv_sql, (f"{uuid.uuid1()}", bank_id, symbol_id, ltv, quote_time, datetime.datetime.now()))
    return add_res


def get_all_bank_ltv():
    sql = f"select a.*,b.bank_code,en_bank_name,cn_bank_name,c.symbol,c.isin,c.country_code,c.exchanger " \
          f"from (TB_Bank_Ltv as a inner join TB_Bank as b on a.bank_id=b.uuid) inner join TB_Symbol as c on a.symbol_id=c.uuid"
    add_res = query_dict(sql)
    return add_res
