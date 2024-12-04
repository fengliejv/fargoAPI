import json
import os
import time
import re

from pip._vendor import requests
from pymysql.converters import escape_string
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from service.ReportService import *

SEEKING_ALPHA = "https://seekingalpha.com/"
PATH = "/home/ibagents/files/sa"
YEAR22_START_TIME = '2022-01-01 00:00:00'
YEAR22_END_TIME = '2023-01-01 00:00:00'
YEAR22_START_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_START_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
YEAR22_END_TIME_TIMESTRP = int(time.mktime(time.strptime(YEAR22_END_TIME, '%Y-%m-%d %H:%M:%S'))) * 1000
NOW_TIMESTRP = int(time.time())
UBS_PDF_DOWNLOAD_URL = f'{SEEKING_ALPHA}api/super-grid-provider-research/v1/document/'

from selenium import webdriver
from selenium.webdriver.common.keys import Keys

chrome_options = Options()
chrome_options.add_argument('--enable-javascript')
chrome_options.add_argument("--nogpu")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1280,1280")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
driver = webdriver.Chrome(options = chrome_options)
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
            """
        })
driver.get("https://seekingalpha.com/symbol/JD/analysis")
login_btn=driver.find_element(By.XPATH, "//span[@class='bE_f bg_ji bg_jx bg_jM']")
login_btn.click()
time.sleep(5)
print(driver.page_source)
emali = driver.find_element(by=By.NAME, value="email")
time.sleep(5)
emali.send_keys("dachein.x@gmail.com")
password=driver.find_element(by=By.NAME, value="password")
time.sleep(5)
password.send_keys("ASdf789123")
print(driver.page_source)
submit_button = driver.find_element(By.XPATH, "//button[@type='submit']")
submit_button.click()




print(driver.page_source)