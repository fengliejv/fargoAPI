import json
from selenium import webdriver
from selenium.webdriver import DesiredCapabilities, chrome
import os,time


def get_log_options():
    option = webdriver.ChromeOptions()
    option.add_argument('--no-sandbox')
    
    option.add_argument("--disable-extensions")
    option.add_argument('--disable-infobars')  
    option.add_argument("--allow-running-insecure-content")
    option.add_argument("--ignore-certificate-errors")
    option.add_argument("--disable-single-click-autofill")
    option.add_argument("--disable-autofill-keyboard-accessory-view[8]")
    option.add_argument("--disable-full-form-autofill-ios")
    option.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:55.0) Gecko/20100101 Firefox/55.0')
    return option


def get_caps():
    caps = DesiredCapabilities.CHROME
    caps['loggingPrefs'] = {
        'browser': 'ALL',
        'performance': 'ALL',
    }
    caps['perfLoggingPrefs'] = {
        'enableNetwork': True,
        'enablePage': False,
        'enableTimeline': False
    }
    return caps



def get_xhr_logs(chrome):
    log_xhr_array = []
    for typelog in chrome.log_types:
        perfs = chrome.get_log(typelog)
        for row in perfs:
            log_data = row
            message_ = log_data['message']
            try:
                log_json = json.loads(message_)
                log = log_json['message']
                if log['method'] == 'Network.responseReceived':
                    
                    type_ = log['params']['type']
                    if type_ == "XHR":
                        log_xhr_array.append(log)
            except:
                pass
    return log_xhr_array



def get_xhr_body(driver, requestId):
    response_body = driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': requestId})
    return response_body


def wait_until_xhr_do(url='',limit = 10):
    tick = 0
    while tick < limit:
        logs = get_xhr_logs(chrome)
        if url == '':
            if len(logs) > 0:
                return logs
        else:
            for log in logs:
                if url in logs['params']['response']['url']:
                    return logs
        tick = tick + 1
    return []


if __name__ == '__main__':
    
    options = get_log_options()
    
    desired_capabilities = get_caps()
    
    chrome = webdriver.Chrome(options=options)
    chrome.get("https://seekingalpha.com/")  
    chrome.maximize_window()