
import csv
import datetime
import uuid

from api.v1.lib.common.mysqlsingle import execute, query_dict
from api.v1.service.TickerService import add_ticker, add_ticker_name


def print_company_code():
    file = open("company.txt", "r")  
    content = file.readlines()  
    company_list = []
    for line in content:  
        line = line.strip('\n')
        company_list.append(line)
    line = 2
    count = 1
    for i in company_list:
        sql = f"select company_code from TB_Script_CompanyNameCode where symbol='{i}' and platform='gs' limit 1"
        data = query_dict(sql)
        
        if len(data) > 0:
            if data[0]['company_code']:
                print(f"{data[0]['company_code']}")
            else:
                print(f"{count}")
                count = count + 1
        line = line + 1


def print_company_id_name():
    file = open("company.txt", "r")  
    content = file.readlines()  
    company_list = []
    for line in content:  
        line = line.strip('\n')
        company_list.append(line)
    line = 2
    data = []
    for i in company_list:
        sql = f"select company_id,b.en_name from TB_Script_CompanyNameCode as a inner join TB_Company as b on a.company_id=b.id  where symbol=%s and platform='gs' and company_code='' limit 1"
        get_data = query_dict(sql, (i,))
        
        if len(get_data) > 0:
            data.append([i, get_data[0]['company_id'], get_data[0]['en_name']])
            print(f"{get_data[0]['company_id']}")
        else:
            data.append([i, "", ""])
        line = line + 1

    file = open("result2.csv", "w", newline='', encoding='utf-8')
    writer = csv.writer(file)
    writer.writerows(data)
    file.flush()
    file.close()
    print(data)


def update_gs_company_code():
    count = 1
    with open("search_result2.csv", "r", encoding="utf-8") as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            try:
                print(count)
                count = count + 1
                if str(row[4]) == "":
                    continue
                if not str(row[4]) == "not found":
                    sql = f"update TB_Script_CompanyNameCode set company_code='{row[4]}' where platform='gs' and company_id='{row[1]}' and symbol='{row[0]}'"
                    data = execute(sql)
                    
                    if data > 0:
                        print(f"{row}")

            except Exception as e:
                print(str(e))

        csvfile.close()


if __name__ == '__main__':
    print_company_code()
    
    
    
    
    
    
