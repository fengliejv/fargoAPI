from lib.Common.mysqlsingle import query_dict, execute


def get_system_variable(key):
    sql = f"SELECT * FROM TB_Config WHERE name=%s limit 1"
    return query_dict(sql, (key,))


def update_system_variable(name, value):
    sql = f"update TB_Config set value=%s WHERE name=%s"
    return execute(sql, (value, name))
