import uuid

from lib.mysql import query_dict, execute, tx_execute


def add_message(uuid, conversation_id, query, answer, time):
    sql = "insert into tb_message(uuid,query,answer,conversation_id,created_time,updated_time) values (%s,%s,%s,%s,%s,%s)"
    return execute(sql, (uuid, query, answer, conversation_id, time, time))


def get_messages(conversation_id, first_id=None, limit=20):
    if first_id:
        sql = "select * from tb_message where uuid=%s"
        message = query_dict(sql, (first_id,))
        if message:
            message = message[0]
        sql = f"select * from tb_message where conversation_id=%s and created_time<%s order by created_time desc limit %s"
        return query_dict(sql, (conversation_id, message['created_time'], limit))
    else:
        sql = f"select * from tb_message where conversation_id=%s order by created_time desc limit %s"
        return query_dict(sql, (conversation_id, limit))


def get_messages_by_ids(message_id_list, conversation_id):
    sql = f"select * from tb_message where uuid in %s and conversation_id=%s"
    return query_dict(sql, (tuple(message_id_list), conversation_id))


def add_share_messages(messages, share_conversation_id):
    sqls = []
    values = []
    for m in messages:
        sql = "insert into tb_message(uuid,conversation_id,created_time,query,answer) values (%s,%s,%s,%s,%s)"
        value = (f"{uuid.uuid1()}", share_conversation_id, m['created_time'], m['query'], m['answer'])
        sqls.append(sql)
        values.append(value)
    return tx_execute(sqls, values)


def add_share_user_messages(share_conversation_id, new_conversation_id):
    sql = """
       INSERT INTO tb_message (uuid,conversation_id,created_time,query,answer)
       SELECT UUID() as uuid,%s as conversation_id,created_time,query,answer  
       FROM tb_message
       WHERE conversation_id=%s;
       """
    return execute(sql, (new_conversation_id, share_conversation_id))
