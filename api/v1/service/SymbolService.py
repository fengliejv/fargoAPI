import datetime
import uuid

from lib.Common.mysqlsingle import query_dict, execute


def get_all_symbol():
    sql = f"select * from TB_Symbol"
    add_res = query_dict(sql)
    return add_res


def get_all_symbol_simple():
    sql = f"select symbol,cn_name,markets_code from TB_Symbol"
    add_res = query_dict(sql)
    return add_res


def add_symbol(symbol, isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger):
    sql = f"insert into TB_Symbol(uuid,symbol,isin,cn_name,en_name,country_code,country,bbg_code,ticker,ric,exchanger,create_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    add_res = execute(sql, (f"{uuid.uuid1()}",
                            symbol, isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger,
                            datetime.datetime.now()))
    return add_res


def update_symbol(symbol_id, isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger):
    sql = f"update TB_Symbol set isin=%s,cn_name=%s,en_name=%s,country_code=%s,country=%s,bbg_code=%s,ticker=%s,ric=%s,exchanger=%s,update_time=%s where uuid=%s"
    return execute(sql,
                   (isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger,
                    datetime.datetime.now(), symbol_id))


def get_symbol_search(symbol, ticker, isin, ric, bbg_code, en_name, cn_name):
    sql = "SELECT * FROM TB_Symbol WHERE 1=1"
    params = []
    if symbol:
        sql += " AND symbol LIKE %s"
        params.append(f"%{symbol}%")
    if ticker:
        sql += " AND ticker LIKE %s"
        params.append(f"%{ticker}%")
    if isin:
        sql += " AND isin LIKE %s"
        params.append(f"%{isin}%")
    if ric:
        sql += " AND ric LIKE %s"
        params.append(f"%{ric}%")
    if bbg_code:
        sql += " AND bbg_code LIKE %s"
        params.append(f"%{bbg_code}%")
    if en_name:
        sql += " AND en_name LIKE %s"
        params.append(f"%{en_name}%")
    if cn_name:
        sql += " AND cn_name LIKE %s"
        params.append(f"%{cn_name}%")
    add_res = query_dict(sql, params)
    return add_res
