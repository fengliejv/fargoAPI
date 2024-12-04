import base64
import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute


def add_send_msg(source, msg_from, msg_to, msg):
    sql = f"insert into TB_Msg_History(uuid,source,msg_from,msg_to,msg,create_time) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql, (
    f"{uuid.uuid1()}", source, msg_from, msg_to, base64.b64encode(msg.encode("utf-8")), datetime.datetime.now()))
