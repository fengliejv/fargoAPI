import datetime
import uuid

from lib.Common.mysqlsingle import query_dict, execute


def get_all_symbol():
    sql = f"select * from TB_Symbol"
    add_res = query_dict(sql)
    return add_res


def add_symbol(symbol, isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger, currency,
               sf_code, gs_code, quartr_code, sa_code, markets_code, is_ETF, is_COM):
    sql = f"""
    insert into TB_Symbol(
    uuid,
    symbol,
    isin,
    cn_name,
    en_name,
    country_code,
    country,
    bbg_code,
    ticker,
    ric,
    exchanger,
    currency,
    sf_code,
    gs_code,
    quartr_code,
    sa_code,
    markets_code,
    is_ETF,
    is_COM,
    create_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    add_res = execute(sql, (
        f"{uuid.uuid1()}", symbol, isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger,
        currency, sf_code, gs_code, quartr_code, sa_code, markets_code, is_ETF, is_COM, datetime.datetime.now()))
    return add_res


def update_symbol(symbol_id, isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger, currency,
                  sf_code, gs_code, quartr_code, sa_code, markets_code, is_ETF, is_COM):
    sql = f"""
    update TB_Symbol set 
    isin=%s,
    cn_name=%s,
    en_name=%s,
    country_code=%s,
    country=%s,
    bbg_code=%s,
    ticker=%s,
    ric=%s,
    exchanger=%s,
    currency=%s,
    sf_code=%s,
    gs_code=%s,
    quartr_code=%s,
    sa_code=%s,
    markets_code=%s,
    is_ETF=%s,
    is_COM=%s,
    update_time=%s where uuid=%s
    """
    return execute(sql,
                   (isin, cn_name, en_name, country_code, country, bbg_code, ticker, ric, exchanger,
                    currency, sf_code, gs_code, quartr_code, sa_code, markets_code, is_ETF, is_COM,
                    datetime.datetime.now(), symbol_id))
