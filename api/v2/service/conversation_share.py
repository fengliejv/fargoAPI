from lib.mysql import execute, query_dict


def add_share(share_id, share_user_id, share_conversation_id, now_time):
    sql = "insert into tb_conversation_share(uuid,share_user_id,share_conversation_id,created_time) values (%s,%s,%s,%s)"
    return execute(sql, (share_id, share_user_id, share_conversation_id, now_time))


def get_conversation_id_from_share(share_id):
    sql = f"select a.*,b.name from tb_conversation_share as a inner join tb_conversation as b on a.share_conversation_id=b.uuid where a.uuid=%s"
    return query_dict(sql, (share_id,))
