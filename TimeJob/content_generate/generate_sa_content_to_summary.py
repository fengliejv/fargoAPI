import datetime
import io
import json

from bs4 import BeautifulSoup

from service.ReportService import add_fatal_log, add_info_log, get_file_basic_not_generate_summary_by_source, \
    add_tb_file_basic_summary_record

SOURCE = "sa"

def generate_sa_content_to_summary():
    all_file_basic = get_file_basic_not_generate_summary_by_source(source=SOURCE)
    page = 1
    while len(all_file_basic) > 0 and page < 200:
        for file in all_file_basic:
            try:
                
                
                o_file = io.open(file['local_save_path'], mode='r', encoding='utf-8')
                f_content = o_file.read()
                o_file.close()
                
                responseHtml = BeautifulSoup(f_content, 'html.parser')
                script_tags = responseHtml.find_all('script')
                if len(script_tags) < 5:
                    continue
                script_tag = script_tags[4]
                if len(script_tag.string) < 19:
                    continue
                match = str(script_tag.string)[18:len(script_tag.string) - 1]
                data = json.loads(match)
                summary_t = json.dumps(data['article']['response']['data']['attributes']['summary'])
                s = summary_t.replace('\\n', "").strip('`').replace('json', '').replace('plaintext', "").strip(
                    "\n").strip()
                if len(s) < 2:
                    continue
                if s[0] == '[':
                    s = s[1:len(s)]
                if s[len(s) - 1] == ']':
                    s = s[0:len(s) - 1]
                if len(s) < 2:
                    continue
                if s[0] == '"':
                    s = s[1:len(s)]
                if s[len(s) - 1] == '"':
                    s = s[0:len(s) - 1]
                ss = s.split("\", \"")
                for d in ss:
                    d = d.strip()
                    res = add_tb_file_basic_summary_record(file_basic_id=file['uuid'], summary=d)
                    if res:
                        pass
                add_info_log(message=f"parse success,TB_File_Basic_id：{file['uuid']}")

            except Exception as e:
                add_fatal_log(message=f"parse fail,TB_File_Basic_id：{file['uuid']},error:{str(e)}")
        page += 1
        all_file_basic = get_file_basic_not_generate_summary_by_source(source=SOURCE)
    return

if __name__ == '__main__':
    generate_sa_content_to_summary()
