import json

from selenium import webdriver
import time

from selenium.webdriver.common.by import By


def get_log_options():
    option = webdriver.ChromeOptions()
    option.add_argument('--no-sandbox')
    option.add_argument("--disable-dev-shm-usage")
    option.add_argument('--headless')








    return option


def gs_get_cookies():

    options = get_log_options()

    chrome = webdriver.Chrome(options=options)
    chrome.get("https://marquee.gs.com/s/home")
    time.sleep(20)
    input = chrome.find_element(By.NAME, value="username")
    input.send_keys("research@goldhorse.com.hk")
    time.sleep(10)
    enter = chrome.find_element(By.TAG_NAME, value="button")
    time.sleep(10)
    enter.click()
    time.sleep(10)
    password = chrome.find_element(By.NAME, value="password")
    password.send_keys("Fargo_2024")
    time.sleep(10)
    enter = chrome.find_elements(By.TAG_NAME, value="button")[1]
    enter.click()
    time.sleep(20)
    cookies = chrome.get_cookies()
    chrome.close()
    COOKIES = ""
    bm_sv = ""
    keys = {}
    for cookie in cookies:
        keys[cookie['name']] = cookie['value']
        if cookie['name'] == 'bm_sv':
            bm_sv = cookie['value']
    timestamp = int(time.time() * 1000)
    expire = timestamp + 2648691
    with open("/home/ibagents/bugs/TimeJob/scrap/gs_cookies.json", 'r', encoding='UTF-8') as f:
        result = json.load(f)
        result['bm_sv'] = bm_sv
        result['_dd_s'] = result['_dd_s'] + f"&created={timestamp}&expire={expire}"
        for key in result:
            if key in keys.keys():
                result[key] = keys[key]
        for key in result:
            COOKIES = COOKIES + f"{key}={result[key]};"
    if len(COOKIES) == 0:
        return COOKIES
    else:
        return COOKIES[0:len(COOKIES)]


if __name__ == '__main__':
    print(gs_get_cookies())
