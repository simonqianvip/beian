#-*- coding:utf-8 -*-

import scrapy
from scrapy.selector import Selector
from scrapy import cmdline
from selenium import webdriver
from scrapy.http import Request, FormRequest
import logging
import codecs
from beian.items import BeianItem
from beian.util.mysql_util import SqlUtil

logger = logging.getLogger(__name__)
LOCALDIR = 'd:\\BEIAN\\'

class BeianSpider(scrapy.Spider):
    def __init__(self):
        self.sqlutil = SqlUtil('127.0.0.1', 'root', '123456', 'fm')
        self.sqlutil.connect()
        logger.info(u'初始化数据库连接')

    name = "beian"
    allowed_domains = ["beian.hndrc.gov.cn"]
    start_urls = [
        'http://beian.hndrc.gov.cn/'
    ]

    def parse(self, response):
        print response.url
        driver = webdriver.PhantomJS()
        driver.get(response.url)
        # driver.implicitly_wait(10)
        # 获取当前窗口
        current_window_handle = driver.current_window_handle

        # TODO 一级页操作
        flag = True
        pageIndex = 1
        items = []
        while(flag):
            logger.info(u'------第%d页------'%pageIndex)
            try:
                lis = driver.find_elements_by_xpath('//*[@id="projectpublicity"]/ul/li')
                for i in range(1,lis.__len__()+1):
                    href = driver.find_element_by_xpath('//*[@id="projectpublicity"]/ul/li[%d]/a'%i)
                    title = driver.find_element_by_xpath('//*[@id="projectpublicity"]/ul/li[%d]/a/div[1]'%i)
                    addr = driver.find_element_by_xpath('//*[@id="projectpublicity"]/ul/li[%d]/a/div[2]'%i)
                    date = driver.find_element_by_xpath('//*[@id="projectpublicity"]/ul/li[%d]/a/div[3]'%i)
                    url = href.get_attribute('href')
                    splits = str(url).split('=')
                    titleId = splits[splits.__len__()-1]
                    # print 'titleTag = %s , addr = %s ,date = %s ,href = %s ,titleId = %s'%(title.text,addr.text,date.text,url,titleId)

                    # TODO mysql数据库查询
                    query_sql = '''select * from beian_info where link = "%s" ''' %url
                    results = self.sqlutil.get_data_from_db(query_sql)

                    if results:
                        logger.info('------database is exist------')
                    else:
                        logger.info('------into second page------')
                        # TODO 二级页操作
                        driver.find_element_by_xpath('//*[@id="projectpublicity"]/ul/li[%d]/a'%i).click()
                        allHandles = driver.window_handles

                        for h in allHandles:
                            if h != current_window_handle:
                                # 切换到二级页面
                                driver.switch_to_window(h)

                        center = driver.find_element_by_xpath('/html/body/center')
                        # TODO 写入到txt文件
                        content = center.text

                        fileName = LOCALDIR + titleId + '.txt'
                        f = codecs.open(fileName, 'wb', encoding='utf-8')
                        f.write(content)
                        f.close()
                        logger.info(U'《%s》写入成功'%fileName)

                        # 关闭当前的二级窗口
                        driver.close()
                        # 切换到一级页面
                        driver.switch_to_window(current_window_handle)

                        item = BeianItem()
                        '''
                        title = scrapy.Field()
                        link = scrapy.Field()
                        date = scrapy.Field()
                        titleId = scrapy.Field()
                        addr = scrapy.Field()
                        '''
                        item['title'] = title.text
                        item['link'] = url
                        item['date'] = date.text
                        item['titleId'] = titleId
                        item['addr'] = addr.text
                        items.append(item)

            except Exception,e:
                logger.info('error:%s',e)

            # TODO 下一页
            pages = driver.find_elements_by_xpath('//*[@id="Pagination"]/a')
            index = 1
            for page in pages:
                flag = False
                classValue = page.get_attribute('class')
                if classValue == 'next':
                    driver.find_element_by_xpath('//*[@id="Pagination"]/a[%d]'%index).click()
                    flag = True
                    pageIndex = pageIndex + 1
                index = index + 1

        self.sqlutil.disconnect()
        logger.info(u'关闭数据库连接')
        return items

if __name__ == '__main__':
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', 'beian'])