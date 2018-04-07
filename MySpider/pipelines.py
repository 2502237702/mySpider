# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
import codecs
import json
from twisted.enterprise import adbapi
from scrapy.exporters import JsonItemExporter
from scrapy.pipelines.images import ImagesPipeline


class MyspiderPipeline(object):
    def process_item(self, item, spider):
        return item


# 异步操作mysql插入
class MysqlTwistedPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbparams = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True
        )

        dbpool = adbapi.ConnectionPool('pymysql', **dbparams)

        return cls(dbpool)

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)

    def handle_error(self, failure, item, spider):
        print(failure)


# 采用同步的机制写入mysql
class MysqlPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(
            '127.0.0.1',
            'liu',
            '123456',
            'myspider',
            charset="utf8",
            use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql, params = item.get_insert_sql()
        self.cursor.execute(insert_sql, params)


# 自定义的将伯乐在线内容保存到本地json的pipeline,可惜失败了
class JsonWithEncodingPipeline(object):
    # 自定义json文件的导出
    def __init__(self):
        # 使用codecs打开避免一些编码问题。
        self.file = codecs.open('article.json', 'w', encoding="utf-8")

    def process_item(self, item, spider):
        # 将item转换为dict,然后调用dumps方法生成json对象，false避免中文出错
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        return item

    # 当spider关闭的时候: 这是一个spider_closed的信号量。
    def spider_closed(self, spider):
        self.file.close()


# 调用scrapy提供的json export导出json文件
class JsonExporterPipeline(object):
    def __init__(self):
        self.file = open('articleexport.json', 'wb')
        self.exporter = JsonItemExporter(
            self.file, encoding="utf-8", ensure_ascii=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()


class ArticleImagePipeline(ImagesPipeline):
    # 重写该方法可从result中获取到图片的实际下载地址
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            for k, v in results:
                image_file_path = v["path"]
            item["front_image_path"] = image_file_path
        return item


class ElasticsearchPipeline(object):
    # 将数据写入到es中
    def process_item(self, item, spider):
        # 将item转换为es的数据
        item.save_to_es()

        return item








