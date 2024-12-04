from lib.mysql import query_dict, execute


def get_attribute_by_research_id(research_id, attribute):
    sql = f"select * from TB_Research_Attribute where research_id=%s and attribute=%s"
    return query_dict(sql, (research_id, attribute))


def update_attribute(attribute_id, value):
    sql = f"update TB_Research_Attribute set value=%s where uuid=%s"
    return execute(sql, (value, attribute_id))