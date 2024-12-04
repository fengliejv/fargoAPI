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


def ubs_get_cookies():
    
    options = get_log_options()

    chrome = webdriver.Chrome(options=options)
    chrome.get("https://neo.ubs.com/static/login.html")
    time.sleep(20)
    input = chrome.find_element(By.ID, value="email_input")
    input.send_keys("Jefferson.sun@goldhorse.com.hk")
    time.sleep(10)
    enter = chrome.find_element(By.NAME, value="email_submit_btn")
    time.sleep(10)
    enter.click()
    time.sleep(10)
    password = chrome.find_element(By.NAME, value="password_input")
    password.send_keys("nM2q6P4hjjQ.EL3")
    time.sleep(10)
    enter = chrome.find_elements(By.NAME, value="password_submit_btn")[0]
    enter.click()
    time.sleep(20)
    cookies = chrome.get_cookies()
    chrome.close()
    COOKIES = ""
    UBS_NEO_AUTH = ""
    for cookie in cookies:
        if cookie['name'] == 'UBS_NEO_AUTH':
            UBS_NEO_AUTH = cookie['value']
    with open("/home/ibagents/bugs/TimeJob/scrap/ubs_cookies.json", 'r', encoding='UTF-8') as f:
        result = json.load(f)
        result['UBS_NEO_AUTH'] = UBS_NEO_AUTH
        for key in result:
            COOKIES = COOKIES + f"{key}={result[key]};"
    if len(COOKIES)==0:
        return COOKIES
    else:
        return COOKIES[0:len(COOKIES)]


if __name__ == '__main__':
    print(ubs_get_cookies())
