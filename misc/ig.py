# Databricks notebook source
# MAGIC %md [跟著IG潮流來爬蟲！如何爬取動態加載網頁（附Python程式碼）](https://medium.com/marketingdatascience/%E8%B7%9F%E8%91%97ig%E6%BD%AE%E6%B5%81%E4%BE%86%E7%88%AC%E8%9F%B2-%E5%A6%82%E4%BD%95%E7%88%AC%E5%8F%96%E5%8B%95%E6%85%8B%E5%8A%A0%E8%BC%89%E7%B6%B2%E9%A0%81-%E9%99%84python%E7%A8%8B%E5%BC%8F%E7%A2%BC-31e99487edca)

# COMMAND ----------

# MAGIC %pip install selenium

# COMMAND ----------

# MAGIC %pip install bs4

# COMMAND ----------

from selenium import webdriver
from bs4 import BeautifulSoup as Soup
import time

browser = webdriver.Chrome()  
url = 'https://www.instagram.com/bbcnews/'
browser.get(url) # 前往該網址

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

# COMMAND ----------


