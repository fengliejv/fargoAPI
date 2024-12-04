import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict, query


def get_original_summary(file_id, version):
    sql = f"select summary from TB_File_Parsed_Original_Summary where file_id=%s and version=%s limit 1"
    data = query_dict(sql, (file_id, version))
    return data
