import datetime
import uuid

from api.v1.lib.common.mysqlsingle import query_dict, execute


def get_all_quartr_by_symbol(start_time, end_time, symbol):
    sql = """
    SELECT t1.*, b.symbol
    FROM (
        SELECT a.*
        FROM TB_Quartr_Calendar a
        JOIN (
            SELECT event_id, MAX(create_time) AS min_create_time
            FROM TB_Quartr_Calendar
            GROUP BY event_id
        ) t2
        ON a.event_id = t2.event_id AND a.create_time = t2.min_create_time
        ORDER BY a.create_time
    ) t1
    INNER JOIN TB_Script_CompanyNameCode b
    ON t1.company_id = b.company_code
    WHERE b.platform = 'quartr'
    AND b.symbol=%s
    AND t1.event_time >= %s
    AND t1.event_time <= %s
    """
    add_res = query_dict(sql, (symbol, start_time, end_time))
    return add_res


def get_all_quartr_by_event_id(event_id):
    sql = """
    SELECT t1.*, b.symbol
    FROM (
        SELECT a.*
        FROM TB_Quartr_Calendar a
        JOIN (
            SELECT event_id, MAX(create_time) AS min_create_time
            FROM TB_Quartr_Calendar where event_id=%s 
            GROUP BY event_id
        ) t2
        ON a.event_id = t2.event_id AND a.create_time = t2.min_create_time AND a.event_id=%s
        ORDER BY a.create_time
    ) t1
    INNER JOIN TB_Script_CompanyNameCode b
    ON t1.company_id = b.company_code
    WHERE b.platform = 'quartr'
    """
    add_res = query_dict(sql, (event_id, event_id))
    return add_res


def get_all_quartr(start_time, end_time):
    sql = """
    SELECT t1.*,b.symbol
    FROM (
        SELECT a.*
        FROM TB_Quartr_Calendar a
        JOIN (
            SELECT event_id, MAX(create_time) AS min_create_time
            FROM TB_Quartr_Calendar
            GROUP BY event_id
        ) t2
        ON a.event_id = t2.event_id AND a.create_time = t2.min_create_time
        ORDER BY a.create_time
    ) t1
    INNER JOIN TB_Script_CompanyNameCode b
    ON t1.company_id = b.company_code
    WHERE b.platform = 'quartr'
    AND t1.event_time >= %s
    AND t1.event_time <= %s
    """
    add_res = query_dict(sql, (start_time, end_time))
    return add_res


def add_quartr(bank_code, en_bank_name, booking_centre, cn_bank_name, support):
    sql = f"insert into TB_Bank(uuid,bank_code,en_bank_name,booking_centre,cn_bank_name,create_time,support) values (%s,%s,%s,%s,%s,%s,%s)"
    add_res = execute(sql, (f"{uuid.uuid1()}",
                            bank_code, en_bank_name, booking_centre, cn_bank_name, datetime.datetime.now(), support))
    return add_res


def update_quartr(bank_id, en_bank_name, booking_centre, cn_bank_name, support):
    sql = f"update TB_Bank set en_bank_name=%s,update_time=%s,booking_centre=%s,cn_bank_name=%s,support=%s where uuid=%s"
    return execute(sql,
                   (en_bank_name, datetime.datetime.now(), booking_centre, cn_bank_name, support, bank_id))
