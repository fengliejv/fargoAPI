import datetime
import json

from service.MsgQueueService import add_msg_queue, get_msg_queue
from service.ReportService import add_error_log, add_fatal_log
from service.WechatService import get_basic_title_summary
from service.WechatUserSubService import get_wechat_user_sub

SUMMARY_VERSION = "1.2"
TAG = "pre_generate_push_report_msg"


def pre_generate_push_report_msg():
    print(TAG)
    try:
        
        start_time = (datetime.datetime.now() - datetime.timedelta(hours=100)).strftime("%Y-%m-%d %H:%M:%S")
        subs = get_wechat_user_sub()
        user = {}
        symbol_file = {}
        symbol_article = {}
        files = get_basic_title_summary(start_time)
        user_msg = get_msg_queue(type='wechat', action='push_report', create_time=start_time)
        article_symbol = {}
        for file in files:
            if not file['symbol'] in symbol_file:
                symbol_file[file['symbol']] = [file]
            else:
                symbol_file[file['symbol']].append(file)
            if file['article_id'] not in article_symbol:
                article_symbol[file['article_id']] = {file['symbol']}
            else:
                article_symbol[file['article_id']].add(file['symbol'])
        for i in symbol_file:
            article = {}
            for file in symbol_file[i]:
                file['symbol'] = ','.join(article_symbol[file['article_id']])
                article[file['article_id']] = file
            if article:
                symbol_article[i] = article
        
        user_file_set = set()
        for msg in user_msg:
            user_file_set.add(f"{msg['receive_user']}_{msg['tag']}")
        for sub in subs:
            try:
                if sub['symbol'] not in symbol_article:
                    continue
                for article_id in symbol_article[sub['symbol']]:
                    
                    if f"{sub['user_id']}_{article_id}" in user_file_set:
                        continue
                    file = symbol_article[sub['symbol']][article_id]
                    if json.loads(file['info'])['primary_symbol'] not in file['symbol']:
                        continue

                    msg = get_report_format(
                        ticker=file['symbol'],
                        publish_time=file['publish_time'],
                        source=file['source'],
                        title=file['title'],
                        title_en=file['title_cn'],
                        summary=file['summary'],
                        uuid=file['uuid']
                    )
                    add_msg_queue(type='wechat', receiver_user=sub['user_id'], msg=msg, action='push_report',
                                  send_user='sys', tag=f'{article_id}', msg_type='txt')
                    user_file_set.add(f"{sub['user_id']}_{article_id}")
            except Exception as e:
                print(f"{str(e)}")
                add_error_log(e=e, message=f"{TAG},fatal:{str(e)}")
    except Exception as e:
        print(f"{str(e)}")
        add_fatal_log(e=e, message=f"{TAG},fatal:{str(e)}")


def get_report_format(ticker, publish_time, source, title, title_en, summary, uuid):
    p = source
    if source == "ubs":
        p = "瑞银"
    elif source == "gs":
        p = "高盛"
    elif source == "ms":
        p = "摩根士丹利"
    if 'zh_CN' in summary:
        return f"Ticker: {ticker}\n" \
               f"Publish time: {publish_time}\n" \
               f"Source: {p}\n\n" \
               f"{title} {title_en}\n\n" \
               f"Link: https://files.fargoinsight.com/file/{uuid}.pdf"
    else:
        return f"Ticker: {ticker}\n" \
               f"Publish time: {publish_time}\n" \
               f"Source: {p}\n\n" \
               f"{title} {title_en}\n\n" \
               f"{summary}\n\n" \
               f"Link: https://files.fargoinsight.com/file/{uuid}.pdf"


if __name__ == '__main__':
    pre_generate_push_report_msg()
