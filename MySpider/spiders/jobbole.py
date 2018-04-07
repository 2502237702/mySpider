# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.loader import ItemLoader
from urllib import parse
from selenium import webdriver
from MySpider.utils.common import get_md5
from MySpider.items import JobBoleArticleItem, MyItemLoader


class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts']

    handle_httpstatus_list = [404]

    def __init__(self):
        self.fail_urls = []
        dispatcher.connect(self.handle_spider_cosed, signals.spider_closed)

    def handle_spider_cosed(self, spider, reason):
        self.crawler.stats.set_value('failed_url', ','.join(self.fail_urls))
        pass

    # 使用selenium:
    # def __init__(self):
    #     self.browser = webdriver.Chrome(executable_path="C:/spiderDriver/chromedriver.exe")
    #     super(JobboleSpider, self).__init__()
    #     # 当匹配到spider_closedz这个信号时。关闭浏览器
    #     dispatcher.connect(self.spider_closed, signals.spider_closed)
    #
    # def spider_closed(self, spider):
    #     # 当爬虫退出的时候关闭chrome
    #     print("spider closed")
    #     self.browser.quit()

    def parse(self, response):
        if response.status == 404:
            self.fail_urls.append(response.url)
            self.crawler.stats.inc_value('failed_url')

        post_nodes = response.css("#archive .floated-thumb .post-thumb a")
        for post_node in post_nodes:
            image_url = post_node.css("img::attr(src)").extract_first('')
            post_url = post_node.css("::attr(href)").extract_first('')
            yield Request(parse.urljoin(response.url, post_url), meta={"front_image_url": image_url}, callback=self.parse_detail)

        next_url = response.css(".next.page-numbers::attr(href)").extract_first('')

        if next_url:
            yield Request(parse.urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):

        front_image_url = response.meta.get("front_image_url", "")

        item_loader = MyItemLoader(item=JobBoleArticleItem(), response=response)
        item_loader.add_css("title", ".entry-header h1::text")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("create_date", "p.entry-meta-hide-on-mobile::text")
        item_loader.add_value("front_image_url", [front_image_url])
        item_loader.add_css("praise_nums", ".vote-post-up h10::text")
        item_loader.add_css("comment_nums", "a[href='#article-comment'] span::text")
        item_loader.add_css("fav_nums", ".bookmark-btn::text")
        item_loader.add_css("tags", "p.entry-meta-hide-on-mobile a::text")
        item_loader.add_css("content", "div.entry")

        article_item = item_loader.load_item()

        yield article_item











