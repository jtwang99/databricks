# Databricks notebook source
# MAGIC %md ##[跟著IG潮流來爬蟲！如何爬取IG貼文短連結](https://medium.com/marketingdatascience/%E8%B7%9F%E8%91%97ig%E6%BD%AE%E6%B5%81%E4%BE%86%E7%88%AC%E8%9F%B2-%E5%A6%82%E4%BD%95%E7%88%AC%E5%8F%96ig%E8%B2%BC%E6%96%87%E7%9F%AD%E9%80%A3%E7%B5%90-%E9%99%84python%E7%A8%8B%E5%BC%8F%E7%A2%BC-d7aae849b2e2)

# COMMAND ----------

# MAGIC %pip install selenium

# COMMAND ----------

# MAGIC %pip install bs4

# COMMAND ----------

from selenium import webdriver
from bs4 import BeautifulSoup as Soup
import time

# COMMAND ----------

# MAGIC %sh ls -l /databricks/driver

# COMMAND ----------

# MAGIC %sh sudo add-apt-repository ppa:canonical-chromium-builds/stage

# COMMAND ----------

# MAGIC %sh  sudo apt update

# COMMAND ----------

# MAGIC %sh sudo apt install chromium-browser

# COMMAND ----------

# MAGIC %sh wget https://chromedriver.storage.googleapis.com/94.0.4606.41/chromedriver_linux64.zip

# COMMAND ----------

# MAGIC %sh unzip chromedriver_linux64.zip

# COMMAND ----------

import os, sys
sys.path.append("/databricks/driver")

# COMMAND ----------

from selenium import webdriver

chrome_path = r'/databricks/driver/chromedriver'
driver = webdriver.Chrome(executable_path=chrome_path)
driver.get('https://www.google.co.in')

# COMMAND ----------

browser = webdriver.Chrome(executable_path='/databricks/driver/chromedriver') 
url = 'https://www.instagram.com/bbcnews/'
browser.get(url) # 前往該網址

# COMMAND ----------

# MAGIC %sh echo $PATH

# COMMAND ----------

sys.path

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# 往下滑並取得新的貼文連結
n_scroll = 5
post_url = []
for i in range(n_scroll):
    scroll = 'window.scrollTo(0, document.body.scrollHeight);'
    browser.execute_script(scroll)
    html = browser.page_source
    soup = Soup(html, 'lxml')

    # 尋找所有的貼文連結
    for elem in soup.select('article div div div div a'):
        # 如果新獲得的貼文連結不在列表裡，則加入
        if elem['href'] not in post_url:
            post_url.append(elem['href'])
    time.sleep(2) # 等待網頁加載

# 總共加載的貼文連結數
print("總共取得 " + str(len(post_url)) + " 篇貼文連結")
