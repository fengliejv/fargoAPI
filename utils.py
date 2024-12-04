import datetime
import json
import re
import uuid

from lib.Common.mysql import query, execute
from lib.Common.mysqlsingle import query_dict

data = ['Nvidia', '招商银行', '网易', '阿里巴巴', 'Alphabet', 'Micron\xa0Technologies', '中国移动', '中石化', 'Block',
        '中芯国际', '香港交易所', 'Carnival', '汇丰控股', '周黑鸭', 'Meta\xa0Platforms', '苹果Apple', 'Zscaler',
        'Netflix', '京东', '台积电', 'Adobe', '小米集团', 'PayPal', '快手', 'Intel', '三一国际', '领展房产基金',
        '迪士尼Disney', 'Capitaland\xa0Ascendas\xa0REIT', '贝壳找房', '中国平安', '蔚来汽车', 'Qualcomm', 'Datadog',
        '友邦保险', 'Wells\xa0Fargo', '腾讯控股', '耐克Nike', '特斯拉 Tesla', '比亚迪',
        'Mapletree\xa0Pan\xa0Asia\xa0Commercial\xa0Trust', 'C3.ai', '小鹏汽车', 'Freeport-McMoRan',
        'MGM\xa0International', '理想汽车', '中国海洋石油', 'Salesforce', 'Walmart', '安踏体育', 'JP\xa0Morgan', '富途',
        'AirBnb', '李宁', '哔哩哔哩', '百度', 'AMD', '紫金矿业', '拼多多', '甲骨文Oracle', 'Boeing', 'Amazon',
        '中国石油股份', '微软Microsoft', '中国宏桥', '万豪Marriot', 'Sea',
        'CapitaLand\xa0Integrated\xa0Commercial\xa0Trust', '美团']


def addCommpany():
    for i in data:
        id = uuid.uuid1()
        sql = f'insert into TB_Company(id,cn_name) values ("{id}","{i}")'
        print(sql)
        print(execute(sql))


def addCommpanyCode(source):
    sql = 'select id,cn_name from TB_Company'
    data = list(query(sql))
    print(data)
    for i in data:
        id = uuid.uuid1()
        sql = f'insert into TB_Script_CompanyNameCode(id,platform,company_id,company_name) values ("{id}","{source}","{i[0]}","{i[1]}")'
        print(sql)
        execute(sql)


def setCommpanyID():
    data = list(query("select id,en_name from TB_Company"))
    for i in data:
        sql = f"update TB_Script_CompanyNameCode set company_id='{i[0]}' where company_name='{i[1]}'"
        print(sql)
        execute(sql)


def setMsCommpanyCode():
    data = list(query("select company_id,company_code,symbol from TB_Script_CompanyNameCode where platform='ubs'"))
    for i in data:
        sql = f"update TB_Script_CompanyNameCode set company_code='{i[1]}',symbol='{i[2]}' where company_id='{i[0]}'"
        print(sql)
        execute(sql)


def resolveCompanyCode():
    with open("companys.txt", 'r', encoding='UTF-8') as f:
        result = json.load(f)
        data = []
        for i in result['rows']:
            if 'o2aZG' in i['data'].keys():
                if i['data']['o2aZG'] and i['data']['aIdqg'] == 'INFO':
                    temp = i['data']['5xYt9'].replace(".US", ".O")
                    data.append(i['data']['yxVVq'] + ":" + temp)
                    sql = f'update TB_Script_CompanyNameCode set company_code="{temp}" where company_name="{i["data"]["yxVVq"]}"'
                    execute(sql)
        print(data)
        print(len(data))


def extract_first_stock_code(input_string):
    regex = r'\(([^\)]*?(\.HK|\.US))[^\)]*\)'
    matches = re.findall(regex, input_string)

    if matches and len(matches) > 0:
        match = matches[0]
        first_code = re.findall(r'[^\(\/]+(\.HK|\.US)', match)[0]

        if '.HK' in first_code:
            first_code = first_code.split('.HK')[0].rjust(4, '0') + '.HK'

        return first_code
    else:
        return ''


def add_new_company():
    
    sql = f'select cn_name,en_name from TB_Company'
    company = query_dict(sql)
    company_en_name = []
    company_cn_name = []
    for com in company:
        company_en_name.append(com["en_name"])
        company_cn_name.append(com["cn_name"])
    file = open("/home/ibagents/bugs/static/stocks.csv", "r")
    file_content = file.readlines()
    com_ubs_dict = dict()
    com_ms_dict = dict()
    for line in file_content:
        line = line.strip('\n')
        temp = line.split(",")
        if temp[1]:
            com_ubs_dict.setdefault(temp[1], temp[7])
            com_ms_dict.setdefault(temp[1], temp[8])

    
    for key in com_ubs_dict:
        if key in company_en_name:
            continue
        if key in company_cn_name:
            continue
        id = uuid.uuid1()
        sql = f'insert into TB_Company(id,cn_name,create_time) values ("{id}","{key}","{datetime.datetime.now()}")'
        print(sql)
        print(execute(sql))


def add_new_stock():
    
    

    sql = f'select cn_name,en_name,id from TB_Company'
    company = query_dict(sql)
    company_name_id = dict()
    for com in company:
        company_name_id.setdefault(com["cn_name"], com["id"])

    sql = f'select symbol from TB_Script_CompanyNameCode'
    stock = query_dict(sql)
    symbol = []

    for com in stock:
        symbol.append(com["symbol"])

    file = open("/home/ibagents/bugs/static/stocks.csv", "r")
    file_content = file.readlines()
    for line in file_content:
        line = line.strip('\n')
        temp = line.split(",")
        if temp[0] in symbol:
            continue
        id = uuid.uuid1()
        sql = f'insert into TB_Script_CompanyNameCode(id,platform,company_code,company_id,company_name,symbol,create_time) ' \
              f'values ("{id}","ubs","{temp[7]}","{company_name_id[temp[1]]}","{temp[1]}","{temp[0]}","{datetime.datetime.now()}")'
        print(sql)
        execute(sql)
        id = uuid.uuid1()
        sql = f'insert into TB_Script_CompanyNameCode(id,platform,company_code,company_id,company_name,symbol,create_time) ' \
              f'values ("{id}","ms","{temp[8]}","{company_name_id[temp[1]]}","{temp[1]}","{temp[0]}","{datetime.datetime.now()}")'
        print(sql)
        execute(sql)
        id = uuid.uuid1()
        sql = f'insert into TB_Script_CompanyNameCode(id,platform,company_code,company_id,company_name,symbol,create_time) ' \
              f'values ("{id}","gs","{temp[9]}","{company_name_id[temp[1]]}","{temp[1]}","{temp[0]}","{datetime.datetime.now()}")'
        print(sql)
        execute(sql)

if __name__ == '__main__':
    
    add_new_stock()
    
