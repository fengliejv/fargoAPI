import json

from selenium import webdriver
from selenium.webdriver import DesiredCapabilities, chrome
import time

from selenium.webdriver.common.by import By


def get_log_options():
    option = webdriver.ChromeOptions()
    option.add_argument("-disable-setuid-sandbox")
    option.add_argument("--disable-dev-shm-usage")
    option.add_argument('--no-sandbox')
    option.add_argument('--headless')  
    option.add_argument("--disable-extensions")
    option.add_argument('--disable-infobars')  
    option.add_argument("--allow-running-insecure-content")
    option.add_argument("--ignore-certificate-errors")
    option.add_argument("--disable-single-click-autofill")
    option.add_argument("--disable-autofill-keyboard-accessory-view[8]")
    option.add_argument("--disable-full-form-autofill-ios")
    option.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:55.0) Gecko/20100101 Firefox/55.0')
    
    
    
    
    
    
    
    
    return option


def ms_get_cookies():
    
    options = get_log_options()

    chrome = webdriver.Chrome(options=options)
    chrome.get("https://ny.matrix.ms.com/")
    time.sleep(20)
    input = chrome.find_element(By.NAME, value="login")
    input.send_keys("jefferson.sun@goldhorse.com.hk")
    time.sleep(10)
    enter = chrome.find_element(By.XPATH, "//input[@class='btn btn-primary pull-right']")
    time.sleep(10)
    enter.click()
    time.sleep(10)
    password = chrome.find_element(By.NAME, value="passwd")
    password.send_keys("Fargo2024")
    time.sleep(10)
    enter = chrome.find_element(By.XPATH, "//input[@class='btn btn-primary pull-right']")
    enter.click()
    time.sleep(20)
    cookies = chrome.get_cookies()
    chrome.close()
    COOKIES = ""
    bm_sv = ""
    keys = {}
    for cookie in cookies:
        keys[cookie['name']] = cookie['value']
    with open("/home/ibagents/bugs/TimeJob/scrap/ms_cookies.json", 'r', encoding='UTF-8') as f:
        result = json.load(f)
        for key in result:
            if key in keys.keys():
                result[key] = keys[key]
        for key in result:
            COOKIES = COOKIES + f"{key}={result[key]};"
    if len(COOKIES) == 0:
        return COOKIES
    else:
        return COOKIES[0:len(COOKIES) - 1]


if __name__ == '__main__':
    print(ms_get_cookies())
