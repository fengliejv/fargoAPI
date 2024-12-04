from selenium import webdriver
from selenium.webdriver import DesiredCapabilities, chrome
import time

from selenium.webdriver.common.by import By

def get_log_options():
    option = webdriver.ChromeOptions()
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


def get_gs_cookies():

    options = get_log_options()

    desired_capabilities = get_caps()

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
    password.send_keys("Fargo_20241")
    time.sleep(10)
    enter = chrome.find_elements(By.TAG_NAME, value="button")[1]
    enter.click()
    time.sleep(20)
    cookies = chrome.get_cookies()
    COOKIES = ""
    for cookie in cookies:
        COOKIES = COOKIES + f"{cookie['name']}:{cookie['value']};"
    return COOKIES


if __name__ == '__main__':
    print(get_gs_cookies())
