
import re

from api.v1.lib.common.mysqlsingle import execute, query_dict


def remove_one_duplicate_path(url):
    
    duplicate_path = "/eqr/article/webapp/services/published/rendition/pdf"
    
    index = url.find(duplicate_path + duplicate_path)
    if index != -1:
        
        cleaned_url = url[:index] + url[index + len(duplicate_path):]
    else:
        cleaned_url = url
    return cleaned_url


def run():
    sql = f"select id,source from TB_File where source like 'https://ny.matrix.ms.com/eqr/article/webapp/services/published/rendition/pdf/eqr/article/webapp/services/published/rendition/pdf/%' order by create_time desc limit 1000"
    data = query_dict(sql)
    if len(data)>0:
        for i in data:
            sql = f"update TB_File set source='{remove_one_duplicate_path(i['source'])}' where id='{i['id']}'"
            print(execute(sql))


if __name__ == '__main__':
    for i in range(0,20):
        run()
