import datetime
import uuid

from lib.Common.mysqlsingle import execute, query_dict


def get_wechat_user_sub():
    sql = f"select * from (select uuid from TB_User) a inner join (select user_id,symbol from TB_User_Sub) b on a.uuid=b.user_id"
    data = query_dict(sql)
    return data
