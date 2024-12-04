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


def cicc_get_cookies():
    count = 0

    options = get_log_options()
    chrome = webdriver.Chrome(options=options)
    chrome.set_page_load_timeout(1800)
    try:
        chrome.get("https://www.research.cicc.com/zh_CN/login?entrance_source=empty")
        time.sleep(30)
        node = chrome.find_elements(By.CLASS_NAME, "el-message-box__btns")
        button = node[0].find_element(By.CLASS_NAME, "el-button")
        button.click()
        chrome.find_element(By.CSS_SELECTOR, ".tab:nth-child(1)").click()
        chrome.find_elements(By.CLASS_NAME, "el-input__inner")[0].send_keys("tiffany.cheung@goldhorse.com.hk")
        chrome.find_elements(By.CLASS_NAME, "el-input__inner")[1].send_keys("Fargo2024!")
        chrome.find_element(By.CSS_SELECTOR, ".el-form:nth-child(3) .el-checkbox__inner").click()
        time.sleep(5)
        chrome.find_element(By.CSS_SELECTOR, ".el-form:nth-child(3) > .el-form-item .el-button").click()
        time.sleep(60)
        cookies = chrome.get_cookies()
        chrome.close()
        keys = {}
        for cookie in cookies:
            keys[cookie['name']] = cookie['value']
        COOKIES = ""
        with open("/home/ibagents/bugs/TimeJob/scrap/cicc_cookies.json", 'r', encoding='UTF-8') as f:
            result = json.load(f)
            for key in result:
                if key in keys.keys():
                    result[key] = keys[key]
            for key in result:
                COOKIES = COOKIES + f"{key}={result[key]};"
        if len(COOKIES) == 0:
            return COOKIES
        else:
            return COOKIES[0:len(COOKIES)]
    except Exception as e:
        print(f'{str(e)} {e.__traceback__.tb_frame.f_globals["__file__"]} line:{e.__traceback__.tb_lineno}')


if __name__ == '__main__':
    print(cicc_get_cookies())
