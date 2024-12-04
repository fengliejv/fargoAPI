import datetime
import uuid

from lib.Common.mysqlsingle import query_dict, execute


def maintain_quartr_company_code():
    sql = "SELECT DISTINCT symbol,company_id,company_name FROM TB_Script_CompanyNameCode"
    data = query_dict(sql)
    data_handle = {}
    for i in data:
        data_handle[i['symbol'].split('.')[0].lower()] = i
    file = open("quartr_export.csv", "r")
    file_content = file.readlines()

    for line in file_content:
        line = line.strip('\n')
        temp = line.split(",")
        if temp[1].lower() in data_handle:
            
            
            sql = f"insert into TB_Script_CompanyNameCode(id,platform,company_id,company_code,company_name,symbol,create_time) values (%s,%s,%s,%s,%s,%s,%s)"
            res = execute(sql, (
                f"{uuid.uuid1()}",
                'quartr',
                data_handle[temp[1].lower()]['company_id'],
                temp[0],
                data_handle[temp[1].lower()]['company_name'],
                data_handle[temp[1].lower()]['symbol'],
                datetime.datetime.now()))
            print(res)
            
            
            
            
            
            
            
            
            
            
            


if __name__ == '__main__':
    maintain_quartr_company_code()
