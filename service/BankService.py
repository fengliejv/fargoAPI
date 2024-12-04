import datetime
import uuid

from lib.Common.mysqlsingle import query_dict, execute


def get_all_bank():
    sql = f"select * from TB_Bank"
    add_res = query_dict(sql)
    return add_res


def add_bank(bank_code, en_bank_name, booking_centre, cn_bank_name, support):
    sql = f"insert into TB_Bank(uuid,bank_code,en_bank_name,booking_centre,cn_bank_name,create_time,support) values (%s,%s,%s,%s,%s,%s,%s)"
    add_res = execute(sql, (f"{uuid.uuid1()}",
                            bank_code, en_bank_name, booking_centre, cn_bank_name, datetime.datetime.now(), support))
    return add_res


def update_bank(bank_id, en_bank_name, booking_centre, cn_bank_name, support):
    sql = f"update TB_Bank set en_bank_name=%s,update_time=%s,booking_centre=%s,cn_bank_name=%s,support=%s where uuid=%s"
    return execute(sql,
                   (en_bank_name, datetime.datetime.now(), booking_centre, cn_bank_name, support, bank_id))
