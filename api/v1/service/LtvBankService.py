import uuid

from api.v1.lib.common.mysqlsingle import query_dict, execute, execute_many, tx_execute


def add_ltv_bank(bank_code, nodes, bank_name):
    node_param = []
    ltv_uuid = uuid.uuid1()
    ltv_sql = f"insert into TB_Ltv_Bank(uuid,bank_code,en_bank_name,create_time) values (%s,%s,%s,%s)"
    add_res = execute(ltv_sql, (ltv_uuid, bank_code, bank_name))
    if not add_res:
        return add_res
    for i in nodes:
        node_param.append((f"{uuid.uuid1()}", ltv_uuid, i['date'], i['stock'], i['ltv'], i['changed']))
    node_sql = f"insert into TB_Ltv_Bank_Stock_Record(uuid,ltv_bank_id,datetime,stock,ltv,changed) values (%s,%s,%s,%s,%s,%s)"
    data = execute_many(node_sql, node_sql)
    return data


def del_ltv_bank(bank_code, bank_uuid):
    sql = "delete from TB_Ltv_Bank where bank_code=%s"
    sql2 = "delete from TB_Ltv_Bank_Stock_Record where ltv_bank_id=%s"
    tx_execute([sql, sql2], [(bank_code,), (bank_uuid,)])


def get_ltv_bank_by_code(bank_code):
    sql = "select * from TB_Ltv_Bank where bank_code=%s"
    return query_dict(sql, (bank_code,))


def get_ltv_bank_all():
    sql = "select * from TB_Ltv_Bank"
    return query_dict(sql)


def get_ltv_bank_node(bank_uuid):
    sql = "select * from TB_Ltv_Bank_Stock_Record where ltv_bank_id=%s"
    return query_dict(sql, (bank_uuid,))


def update_ltv_bank(bank_code, bank_name):
    sql = f"update TB_Ltv_Bank set bank_name=%s where bank_code=%s"
    return execute(sql, (bank_name, bank_code))


def add_bank_ltv(bank, country, isin, underlying, stock_code, bank_ltv, submitter, effect_time, create_time):
    ltv_sql = f"insert into TB_Bank_Ltv(uuid,bank,country,isin,underlying,stock_code,bank_ltv,submitter," \
              f"effect_time,create_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    add_res = execute(ltv_sql, (
        f"{uuid.uuid1()}", bank, country, isin, underlying, stock_code, float(bank_ltv), submitter, int(effect_time),
        int(create_time)))
    return add_res
