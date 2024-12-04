import datetime
import random
from time import sleep

import requests
from service.MsgQueueService import get_msg_queue_pending, set_msg_queue_attr, set_ready_msg
from service.ReportService import add_fatal_log, add_error_log

TAG = "handle_wechat_msg_queue"


def handle_wechat_msg_queue():
    try:
        print(TAG)
        
        user_msg = get_msg_queue_pending(type='wechat', now_time=datetime.datetime.now())
        set_ready_msg()
        print(len(user_msg))
        for msg in user_msg:
            try:
                set_msg_queue_attr(msg_id=msg['uuid'], attr='status', value='sending')
            except Exception as e:
                print(str(e))
        url = "http://212.64.23.164:9530/wechat/send"
        headers = {
            "Authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MTY1MzY4NDkuMzM2MTAzNCwicGxhdGZvcm0iOiJmYXJnb2luc2lnaHQifQ.7RdWCYB19LXtlSyskqGiB06HD0dwU1tKV6S-chUpjXM",
            "content-type": "application/json"
        }
        for msg in user_msg:
            try:
                set_msg_queue_attr(msg_id=msg['uuid'], attr='status', value='sending')
                payload = {
                    "msg": msg['msg'],
                    "user": msg['nick_name'],
                    "type": msg['msg_type']
                }
                
                response = requests.request("POST", url, json=payload, headers=headers, timeout=120)
                if response.status_code == 200 and response.text == "ok":
                    set_msg_queue_attr(msg_id=msg['uuid'], attr='status', value='send_ok')
                else:
                    set_msg_queue_attr(msg_id=msg['uuid'], attr='status', value='send_fail')
                sleep(random.randint(5, 10))
            except Exception as e:
                print(str(e))
                set_msg_queue_attr(msg['uuid'], 'status', 'send_fail')
                add_error_log(e=e, message=f"{TAG} send msg to wechat fail:{str(e)}")
                continue
    except Exception as e:
        print(str(e))
        add_fatal_log(e=e, message=f"{TAG} fail:{str(e)}")


if __name__ == '__main__':
    handle_wechat_msg_queue()
