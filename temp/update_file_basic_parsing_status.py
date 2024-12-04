
import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict
from lib.Common.my_minio import Bucket

if __name__ == '__main__':
    minio_obj = Bucket()
    res1, res2 = None, None
    if minio_obj.exists_bucket("report-parse-result"):
        res = minio_obj.client.list_objects(bucket_name="report-parse-result", prefix="")
        for i in res:
            file_id = i.object_name[0:36]
            print(i.object_name)
            if len(file_id) == 36:
                sql = f"update TB_File_Basic set parse_status='parse_ok' where uuid='{file_id}'"
                res = execute(sql)
                print(f"{file_id}:{res}")
