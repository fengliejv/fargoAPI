import datetime
import uuid

from lib.Common.mysqlsingle import execute, query_dict


def add_bank_exchange_rate(currency, rate, date, timestamp):
    sql = f"delete from TB_Bank_Exchange_Rate where currency=%s"
    execute(sql, (currency,))
    sql = f"insert into TB_Bank_Exchange_Rate(uuid,currency,rate,date,timestamp) values (%s,%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", currency, rate, date, timestamp))
