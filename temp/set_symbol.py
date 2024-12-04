

from api.v1.lib.common.mysqlsingle import execute, query_dict


def run():
    file = open("TB_Company.csv", "r")  
    content = file.readlines()  
    symbol_dict = {}
    for line in content:  
        line = line.replace('"', '')
        line = line.strip('\n')
        temp = line.split(",")
        if temp[5]:
            symbol_dict.setdefault(temp[2], temp[5])

    file = open("TB_Company2.csv", "r")  
    content2 = file.readlines()  
    coms_dict = {}
    for line in content2:  
        line = line.replace('"', '')
        line = line.strip('\n')
        temp = line.split(",")
        if temp[2]:
            coms_dict.setdefault(temp[0], temp[2])
    com_dict = dict()
    for i in coms_dict:
        dir = coms_dict[i].replace(".", "").replace(" ", "").replace("&", "")
        if dir not in com_dict and i in symbol_dict:
            com_dict.setdefault(dir, symbol_dict[i])

    sql = f"select en_name,company_code,symbol from TB_Company as a inner join TB_Script_CompanyNameCode as b on a.id=b.company_id"
    data = query_dict(sql)
    for company in data:
        com_dict.setdefault(company["en_name"].replace(".", "").replace(" ", "").replace("&", ""), company["symbol"])
    print(com_dict)

    for i in range(0, 4):
        sql = "select id,file_path from TB_File where symbol is null order by create_time desc limit 1000 "
        data = query_dict(sql)
        count=0
        for h in data:
            last_index = h['file_path'].rfind("/")
            second_index = h['file_path'].rfind("/", 0, last_index)
            ticker = h['file_path'][second_index + 1:last_index]
            ticker = ticker.replace(".", "").replace(" ", "").replace("&", "")
            symbol = ""
            if ticker in com_dict:
                symbol = com_dict[ticker]
            count += 1
            print(count)
            print(execute(f"update TB_File set symbol='{symbol}' where id='{h['id']}'"))


if __name__ == '__main__':
    run()
