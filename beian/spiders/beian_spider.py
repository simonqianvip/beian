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
import time
import random

logger = logging.getLogger(__name__)
LOCALDIR = 'd:\\BEIAN\\'

class BeianSpider(scrapy.Spider):
    download_count = 0
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
        # 开始时间
        s_time = self.GetNowTime()
        driver = webdriver.PhantomJS()
        driver.get(response.url)
        # driver.implicitly_wait(10)
        # 获取当前窗口
        main_window_handle = driver.current_window_handle
        try:
            # TODO 一级页操作
            flag = True
            pageIndex = 1
            items = []
            while(flag):
                logger.info(u'------第%d页------'%pageIndex)

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

                    # TODO 数据是否存在
                    query_sql = '''select * from beian_info where link = "%s" ''' %url
                    results = self.sqlutil.get_data_from_db(query_sql)

                    if results:
                        logger.info('---------- Database Is Exist ----------')
                    else:
                        logger.info('---------- Into Second Page ----------')
                        # TODO 二级页操作
                        driver.find_element_by_xpath('//*[@id="projectpublicity"]/ul/li[%d]/a'%i).click()
                        # 切换到2级页面
                        self.switch_window(driver, main_window_handle,False)
                        # TODO 等待元素加载
                        time.sleep(5)

                        companyname__text = driver.find_element_by_xpath('//*[@id="companyname"]').text

                        beforehandname__text = driver.find_element_by_xpath('//*[@id="beforehandname"]').text
                        company = None
                        if companyname__text.strip():
                            company = companyname__text
                        elif beforehandname__text.strip():
                            company = beforehandname__text

                        print 'company = %s'%company
                        # 写入到本地文件
                        self.write2File(driver, titleId)
                        # 关闭当前的二级窗口
                        self.switch_window(driver,main_window_handle,True)
                        # 切换到主页面
                        driver.switch_to_window(main_window_handle)

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
                        item['company'] = company
                        items.append(item)

                # TODO 下一页
                flag, pageIndex = self.next_page(driver, flag, pageIndex)
            # 更新爬虫数据
            self.spider_update(items,s_time)

        except Exception,e:
            logger.error(e)
        finally:
            driver.close()
            self.sqlutil.disconnect()
            logger.info(u'关闭数据库连接')
            return items

    def GetNowTime(self):
        """
        获取当前系统时间
        :return:
        """
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def spider_update(self, items, s_time):
        """
        爬虫数据更新
        :param items:
        :param s_time:
        :return:
        """
        e_time = self.GetNowTime()
        u_count = len(items)
        # 插入更新语句
        insert_sql = """
                    insert into spider_info(site,s_time,e_time,update_count,download_count)
                    values('河南企业项目备案网','%s','%s','%s','%s')""" % (s_time, e_time, u_count, self.download_count)
        self.sqlutil.exec_db_cmd(insert_sql)

    def next_page(self, driver, flag, pageIndex):
        """
        获取下一页数据
        :param driver:
        :param flag:
        :param pageIndex:
        :return:
        """
        pages = driver.find_elements_by_xpath('//*[@id="Pagination"]/a')
        index = 1
        for page in pages:
            flag = False
            classValue = page.get_attribute('class')
            if classValue == 'next':
                driver.find_element_by_xpath('//*[@id="Pagination"]/a[%d]' % index).click()
                flag = True
                pageIndex = pageIndex + 1
            index = index + 1
        return flag, pageIndex

    def write2File(self, driver, titleId):
        """
        把获取的信息写入到本地文件
        :param driver:
        :param titleId:
        :return:
        """
        center = driver.find_element_by_xpath('/html/body/center')
        # TODO 写入到txt文件
        content = center.text
        print 'content = %s'%content
        fileName = LOCALDIR + titleId + '.txt'
        f = codecs.open(fileName, 'w', encoding='utf-8')
        f.write(content)
        f.close()
        # 随机休眠2~3秒
        time.sleep(random.randint(2, 3))
        logger.info(U'《%s》写入成功' % fileName)
        self.download_count = self.download_count + 1

    def switch_window(self, driver, main_window_handle,window_flag):
        """
        切换到二级页面
            true:关闭当前window
            false:不关闭当前window
        :param driver:
        :param main_window_handle:
        :param window_flag:
        :return:
        """
        allHandles = driver.window_handles
        for h in allHandles:
            if h != main_window_handle:
                # 切换到二级页面
                driver.switch_to_window(h)
                if window_flag == True:
                    driver.close()


if __name__ == '__main__':
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', 'beian'])