from lib.Common.mysqlsingle import query_dict, execute


def get_file_attribute(file_id):
    sql = f"select attribute from TB_File where id=%s"
    data = query_dict(sql, (file_id,))
    return data
