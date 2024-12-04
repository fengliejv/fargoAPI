from lib.postgres import query_dict


def get_messages(message_id_list, conversation_id):
    sql = f"select * from messages where id in %s where conversation_id=%s"
    return query_dict(sql, (message_id_list, conversation_id))
