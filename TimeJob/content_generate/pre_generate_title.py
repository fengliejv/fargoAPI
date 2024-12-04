import datetime
from api.v1.service.ReportService import add_title
from lib.Common.utils import my_translate_text
from service.FileTitleService import get_file_title_limit_time, get_same_article_title
from service.ReportService import add_error_log, add_fatal_log


def pre_generate_lang_title():
    print("pre_generate_lang_title")
    try:
        
        start_time = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        data = get_file_title_limit_time(start_time)
        generated_title = {}
        print(len(data))
        for i in data:
            data2 = get_same_article_title(article_id=i['article_id'], source=i['source'])
            if len(data2) > 0:
                data2 = data2[0]
                add_title(file_id=i['uuid'], title=data2['title'], lang="zh-CN")
                print(f"same{i['uuid']}{data2['title']}")
            else:
                if f"{i['article_id']}{i['source']}" in generated_title:
                    add_title(file_id=i['uuid'], title=generated_title[f"{i['article_id']}{i['source']}"], lang="zh-CN")
                    continue
                tan_res = my_translate_text(text=i['title'], target_lang="ZH", source_lang="EN")
                if tan_res:
                    generated_title[f"{i['article_id']}{i['source']}"] = tan_res
                    add_title(file_id=i['uuid'], title=tan_res, lang="zh-CN")
                    print(f"generate{i['uuid']}{tan_res}")
                else:
                    print(f"fail{i['uuid']}{tan_res}")
                    add_error_log(message=f"pre_generate_lang_title,error:translate title is {tan_res}")
    except Exception as e:
        add_fatal_log(message=f"pre_generate_lang_title,fatal:{str(e)}")


if __name__ == '__main__':
    pre_generate_lang_title()
