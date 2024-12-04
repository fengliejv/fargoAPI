from lib.mysql import query_dict, execute, tx_execute


def add_conversation(uuid, name, user_id, time, message_id, query, answer):
    sql1 = "insert into tb_conversation(uuid,name,user_id,created_time,updated_time) values (%s,%s,%s,%s,%s)"
    sql2 = "insert into tb_message(uuid,query,answer,conversation_id,created_time,updated_time) values (%s,%s,%s,%s,%s,%s)"
    return tx_execute([sql1, sql2],
                      [(uuid, name, user_id, time, time),
                       (message_id, query, answer, uuid, time, time)])


def set_conversation_name(conversation_id, name):
    sql = "update tb_conversation set name=%s where conversation=%s"
    return execute(sql, (conversation_id, name))


def add_user_conversation_share(conversation_id, name, user_id, now_time):
    sql = "insert into tb_conversation(uuid,name,user_id,created_time,updated_time) values (%s,%s,%s,%s,%s)"
    return execute(sql, (conversation_id, name, user_id, now_time, now_time))


def add_share_conversation(conversation_id, name, now_time):
    sql = "insert into tb_conversation(uuid,name,created_time,updated_time) values (%s,%s,%s,%s)"
    return execute(sql, (conversation_id, name, now_time, now_time))


def get_conversation_by_user_id(user_id, conversation_id):
    sql = "select * from tb_conversation where user_id=%s and uuid=%s"
    return execute(sql, (user_id, conversation_id))


def get_conversation_by_id(conversation_id):
    sql = "select * from tb_conversation where uuid=%s"
    return query_dict(sql, (conversation_id,))


def get_conversations_by_last_id(last_id, limit, user_id):
    if last_id:
        sql = "select * from tb_conversation where uuid=%s and user_id=%s"
        conversation = query_dict(sql, (last_id, user_id))
        if conversation:
            conversation = conversation[0]
        sql = f"select * from tb_conversation where user_id=%s and created_time<%s " \
              f"order by created_time desc limit %s"
        return query_dict(sql, (user_id, conversation['created_time'], limit))
    else:
        sql = f"select * from tb_conversation where user_id=%s order by created_time desc limit %s"
        return query_dict(sql, (user_id, limit))


def delete_conversations_by_id(conversation_id, user_id):
    sql = "delete from tb_conversation where uuid=%s and user_id=%s"
    return execute(sql, (conversation_id, user_id))


def update_conversation_name_by_id(conversation_id, user_id, name):
    sql = "update tb_conversation set name=%s where uuid=%s and user_id=%s"
    return execute(sql, (name, conversation_id, user_id))
