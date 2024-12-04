import json
import time

from selenium.webdriver.common.by import By
from seleniumwire import webdriver


def get_log_options():
    option = webdriver.ChromeOptions()
    option.add_argument("-disable-setuid-sandbox")
    option.add_argument("--disable-dev-shm-usage")
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


def get_quartr_header():
    
    options = get_log_options()

    chrome = webdriver.Chrome(options=options)
    chrome.get("https://web.quartr.com/?_gl=1*bhz6c1*_gcl_au*MTc3NDY2MDk0NC4xNzE4MTY0ODEx")
    time.sleep(60)
    chrome.find_element(By.ID, "email").send_keys("dachein.x@gmail.com")
    time.sleep(10)
    chrome.find_element(By.ID, "password").send_keys("cKzzciR2wNDU3C7")
    time.sleep(10)
    chrome.find_element(By.CSS_SELECTOR, ".ee3rfyo > .\\_1f5pq1z0").click()
    time.sleep(30)
    header_json = {}
    for request in chrome.requests:
        if request.url == 'https://private.quartr.com/api/v2/users/me':
            print(request.headers)
            print(request.headers.get('authorization'))
            header = request.headers
            for i in header.keys():
                header_json[i] = header.get(i)

    return header_json


if __name__ == '__main__':
    print(get_quartr_header())
