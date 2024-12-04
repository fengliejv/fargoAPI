import datetime
import io
import json
from bs4 import BeautifulSoup

from service.ReportService import add_fatal_log, \
    get_all_tb_file_by_publish_time_and_source, add_tb_file_basic_record, add_info_log, get_all_tb_file_basic_by_source, \
    check_repeat_report
from config import Config

SOURCE = "sa"
PATH = f"/home/ibagents/files/{SOURCE}"


def parse_sa_file_to_file_basic():
    file = open("/home/ibagents/bugs/static/sacompany.txt", "r")
    file_content = file.readlines()
    company_list = []
    for line in file_content:
        line = line.strip('\n')
        company_list.append(line)
    file.close()
    
    file = open("/home/ibagents/bugs/static/stock-company.csv", "r")
    file_content = file.readlines()
    com_dict = dict()
    for line in file_content:
        line = line.strip('\n')
        temp = line.split(",")
        if temp[1]:
            com_dict.setdefault(temp[1], temp[0])

    file.close()
    
    all_record = get_all_tb_file_basic_by_source(source=SOURCE, key="article_id")
    if len(all_record) > 0:
        parsing_start_time = list(all_record.values())[0]['publish_time']
    else:
        parsing_start_time = Config.DATA_START_TIME
    
    files = get_all_tb_file_by_publish_time_and_source(publish_time=parsing_start_time, source=SOURCE)
    
    for file in files:
        try:
            article_id = json.loads(file['attribute'])['id']
            if article_id in all_record:
                if all_record[article_id]['publish_time'] == file['publish_time'] and \
                        all_record[article_id][
                            'file_id'] == file['id']:
                    
                    print("already exits")
                    continue
            f = io.open(file['file_path'], mode='r', encoding='utf-8')
            content = f.read()
            file_content = content
            f.close()
            response_html = BeautifulSoup(content, 'html.parser')
            
            content = response_html.find_all('div', attrs={'data-test-id': 'article-content'})
            if len(content) <= 0:
                continue
            content = str(content[0])
            
            author_head = response_html.find('img', attrs={'data-test-id': 'user-pic'})
            author_head = author_head.get("src")
            
            author = response_html.find('a', attrs={'data-test-id': 'author-name'})
            author = author.text
            
            last_index = file['file_path'].rfind("/")
            second_index = file['file_path'].rfind("/", 0, last_index)
            ticker = file['file_path'][second_index + 1:last_index]
            symbol = com_dict.get(ticker.replace(".", "").replace(" ", "").replace("&", ""))
            check_repeat = check_repeat_report(source=SOURCE, article_id=article_id, symbol=symbol)
            if len(check_repeat) > 0:
                continue
            
            res = add_tb_file_basic_record(
                article_id=article_id, file_id=file['id'], author=author, author_head=author_head,
                source=SOURCE, title=file['title'], content=content, lang="en-US", original_url=file['source'],
                local_save_path=file['file_path'], publish_time=file['publish_time'], creator='system',
                create_time=datetime.datetime.now(), symbol=symbol)
            if res:
                print(f"定时解析{SOURCE}报告成功,TB_File_ID：{file['id']}")
                add_info_log(message=f"定时解析{SOURCE}:tb_file_basic成功,TB_File_ID：{file['id']}")

        except Exception as e:
            add_fatal_log(message=f"定时解析{SOURCE}报告异常,TB_File_ID：{file['id']},报错:{str(e)}")


if __name__ == '__main__':
    parse_sa_file_to_file_basic()
