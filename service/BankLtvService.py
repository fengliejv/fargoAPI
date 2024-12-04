import datetime
import time
import uuid

from lib.Common.mysqlsingle import execute, query_dict


def add_bank_ltv(bank_id, symbol_id, ltv, quote_time):
    ltv_sql = f"insert into TB_Bank_Ltv(uuid,bank_id,symbol_id,ltv,quote_time,update_time) values (%s,%s,%s,%s,%s,%s)"
    add_res = execute(ltv_sql, (f"{uuid.uuid1()}", bank_id, symbol_id, ltv, quote_time, datetime.datetime.now()))
    return add_res


def get_all_bank_ltv():
    sql = f"select a.*,b.bank_code,c.symbol from (TB_Bank_Ltv as a inner join TB_Bank as b on a.bank_id=b.uuid) inner join TB_Symbol as c on a.symbol_id=c.uuid"
    add_res = query_dict(sql)
    return add_res


def update_bank_ltv(ltv_id, ltv):
    sql = f"update TB_Ltv_Bank set ltv=%s where uuid=%s"
    return execute(sql, (ltv, ltv_id))
