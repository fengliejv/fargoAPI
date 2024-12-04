import datetime
import uuid

from lib.Common.mysqlsingle import query_dict, execute


def get_not_upload_file():
    sql = f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY uuid) AS row_num FROM TB_File_Basic) t WHERE row_num = 1 and publish_time>'2024-02-27 00:00:00' AND UUID NOT IN (SELECT UUID FROM TB_API_ChatDoc WHERE upload_time>'2024-02-20 00:00:00')"
    data = query_dict(sql)
    return data


def add_upload_record(file_id, upload_id):
    sql = f"insert into TB_API_ChatDoc(uuid,file_id,upload_id,upload_time) values (%s,%s,%s,%s)"
    return execute(sql, (f"{uuid.uuid1()}", file_id, upload_id, datetime.datetime.now()))
