# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import datetime
import re
import redis

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join
from MySpider.utils.common import extract_num
from MySpider.settings import SQL_DATETIME_FORMAT
from w3lib.html import remove_tags
from elasticsearch_dsl.connections import connections
from MySpider.models.es_jobbole import ArticleType
from MySpider.models.es_lagou import LagouType
from MySpider.models.es_zhihu import ZhiHuQuestionType, ZhiHuAnswerType

es = connections.create_connection(LagouType._doc_type.using)

redis_cli = redis.StrictRedis()


class MyspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def date_convert(value):
    try:
        create_date = datetime.datetime.strptime(value, "%Y/%m/%d").date()
    except Exception as e:
        create_date = datetime.datetime.now().date()

    return create_date


def gen_suggests(index, info_tuple):
    # 根据字符串生成搜索建议数组
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es的analyze接口分析字符串
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter': ["lowercase"]}, body=text)
            anylyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"]) > 1])
            new_words = anylyzed_words - used_words
        else:
            new_words = set()

        if new_words:
            suggests.append({"input": list(new_words), "weight": weight})

    return suggests


def exclude_none(value):
    if value:
        return value
    else:
        value = '无'
    return value


def get_nums(value):
    match_re = re.match('.*?(\d+).*', value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0
    return nums


def remove_comment_tags(value):
    if "评论" in value:
        return ""
    else:
        return value


def return_value(value):
    return value


class MyItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class ZhihuQuestionItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field(
        input_processor=MapCompose(exclude_none),
    )
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                    insert into zhihu_question(
                    zhihu_id, topics, url, title, content, answer_num, comments_num, 
                    watch_user_num, click_num, crawl_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    ON DUPLICATE KEY UPDATE content=VALUES(content), 
                    answer_num=VALUES(answer_num), comments_num=VALUES(comments_num), 
                    watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
                """
        zhihu_id = self['zhihu_id'][0]
        topics = ",".join(self["topics"])
        url = self["url"][0]
        title = "".join(self["title"])
        try:
            content = "".join(self["content"])
        except BaseException:
            content = "无"
        try:
            answer_num = extract_num("".join(self["answer_num"]))
        except BaseException:
            answer_num = 0
        comments_num = extract_num("".join(self["comments_num"]))

        if len(self["watch_user_num"]) == 2:
            watch_user_num = extract_num(self["watch_user_num"][0])
            click_num = extract_num(self["watch_user_num"][1])
        else:
            watch_user_num = extract_num(self["watch_user_num"][0])
            click_num = 0

        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (
            zhihu_id,
            topics,
            url,
            title,
            content,
            answer_num,
            comments_num,
            watch_user_num,
            click_num,
            crawl_time)

        return insert_sql, params

    def save_to_es(self):
        zhihu_id = self['zhihu_id'][0]
        topics = ",".join(self["topics"])
        url = self["url"][0]
        title = "".join(self["title"])
        try:
            content = "".join(self["content"])
        except BaseException:
            content = "无"
        try:
            answer_num = extract_num("".join(self["answer_num"]))
        except BaseException:
            answer_num = 0
        comments_num = extract_num("".join(self["comments_num"]))

        if len(self["watch_user_num"]) == 2:
            watch_user_num = extract_num(self["watch_user_num"][0])
            click_num = extract_num(self["watch_user_num"][1])
        else:
            watch_user_num = extract_num(self["watch_user_num"][0])
            click_num = 0

        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        question = ZhiHuQuestionType()
        question.title = title
        question.zhihu_id = zhihu_id
        question.topics = topics
        question.url = url
        question.content = content
        question.answer_num = answer_num
        question.comments_num = comments_num
        question.watch_user_num = watch_user_num
        question.click_num = click_num
        question.crawl_time = crawl_time

        question.suggest = gen_suggests(ArticleType._doc_type.index, ((question.title, 10), (question.topics, 7)))
        question.save()
        redis_cli.incr("zhihu_count")

        return


class ZhihuAnswerItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()
    author_name = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎answer表的sql语句
        insert_sql = """
            insert into zhihu_answer(
            zhihu_id, url, question_id, author_id, content, praise_num, comments_num,
            create_time, update_time, crawl_time,author_name) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s) 
            ON DUPLICATE KEY UPDATE content=VALUES(content), 
            comments_num=VALUES(comments_num), praise_num=VALUES(praise_num),
            update_time=VALUES(update_time), author_name=VALUES(author_name)
        """

        create_time = datetime.datetime.fromtimestamp(
            self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(
            self["update_time"]).strftime(SQL_DATETIME_FORMAT)

        params = (
            self["zhihu_id"],
            self["url"],
            self["question_id"],
            self["author_id"],
            self["content"],
            self["praise_num"],
            self["comments_num"],
            create_time,
            update_time,
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
            self["author_name"],
        )

        return insert_sql, params

    def save_to_es(self):
        create_time = datetime.datetime.fromtimestamp(
            self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(
            self["update_time"]).strftime(SQL_DATETIME_FORMAT)

        answer = ZhiHuAnswerType()
        answer.zhihu_id = self['zhihu_id']
        answer.create_time = create_time
        answer.update_time = update_time
        answer.crawl_time = self['crawl_time'].strftime(SQL_DATETIME_FORMAT)
        answer.url = self['url']
        answer.question_id = self['question_id']
        try:
            answer.author_id = self['author_id']
        except:
            pass
        answer.author_name = self['author_name']
        try:
            answer.praise_num = self['praise_num']
        except:
            pass
        try:
            answer.comments_num = self['comments_num']
        except:
            pass

        answer.suggest = gen_suggests(ZhiHuAnswerType._doc_type.index, ((answer.create_time, 10), (answer.zhihu_id, 7)))
        answer.save()
        redis_cli.incr("zhihu_count")

        return


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert),
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        # 使用自定义的outprocessor覆盖原始的take first 使得image_url是一个列表。
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        # input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        # list使用逗号连接
        output_processor=Join(",")
    )
    content = scrapy.Field()
    crawl_time = scrapy.Field()

    def make_data_clean(self):
        front_image_url = ""
        self["crawl_time"] = datetime.datetime.now(
        ).strftime(SQL_DATETIME_FORMAT)
        if self["front_image_url"]:
            self["front_image_url"] = self["front_image_url"][0]
        str = self["create_date"].strip().replace("·", "").strip()
        self["create_date"] = datetime.datetime.strptime(
            str, "%Y/%m/%d").date()
        nums = 0
        value = self["praise_nums"]
        match_re = re.match(".*?(\d+).*", value)
        if match_re:
            nums = int(match_re.group(1))
        else:
            nums = 0
        self["praise_nums"] = nums

    def get_insert_sql(self):
        insert_sql = """
            insert into jobbole_article(title, url, url_object_id,create_date, fav_nums, 
            front_image_url, front_image_path, praise_nums, 
            comment_nums, tags, content,crawl_time)
            VALUES (%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s) ON DUPLICATE KEY UPDATE 
            fav_nums=VALUES(fav_nums),praise_nums=VALUES(praise_nums),
            comment_nums=VALUES(comment_nums),crawl_time=VALUES(crawl_time)
        """
        self.make_data_clean()
        params = (
            self["title"],
            self["url"],
            self["url_object_id"],
            self["create_date"],
            self["fav_nums"],
            self["front_image_url"],
            self["front_image_path"],
            self["praise_nums"],
            self["comment_nums"],
            self["tags"],
            self["content"],
            self["crawl_time"]
        )
        return insert_sql, params

    def save_to_es(self):
        article = ArticleType()
        article.title = self['title']
        article.create_date = self["create_date"]
        article.content = remove_tags(self["content"]).strip().replace("\r\n", "").replace("\t", "")
        article.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            article.front_image_path = self["front_image_path"]
        try:
            article.praise_nums = self["praise_nums"]
        except:
            pass
        article.fav_nums = self["fav_nums"]
        article.comment_nums = self["comment_nums"]
        article.url = self["url"]
        article.tags = self["tags"]
        article.meta.id = self["url_object_id"]

        article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7)))
        article.save()
        redis_cli.incr("jobbole_count")

        return


class FangItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    price = scrapy.Field()
    address = scrapy.Field()
    tags = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into fang(id, name, price,address, tags, crawl_time)
            VALUES (%s, %s, %s,%s, %s, %s) ON DUPLICATE KEY UPDATE  price=VALUES(price)
        """
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        self["crawl_time"] = crawl_time
        self["name"] = self["name"].strip()
        match_hans5 = re.match(
            ".*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*",
            self["tags"], re.DOTALL)
        match_hans4 = re.match(
            ".*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*",
            self["tags"], re.DOTALL)
        match_hans3 = re.match(
            ".*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*",
            self["tags"], re.DOTALL)
        match_hans2 = re.match(
            ".*>([\u4e00-\u9fa5]+)<.*>([\u4e00-\u9fa5]+)<.*",
            self["tags"], re.DOTALL)
        match_hans1 = re.match(
            ".*>([\u4e00-\u9fa5]+)<.*",
            self["tags"], re.DOTALL)

        if match_hans5:
            self["tags"] = match_hans5.group(1) + "," + match_hans5.group(2) + match_hans5.group(
                3) + "," + match_hans5.group(4) + "," + match_hans5.group(5)
        elif match_hans4:
            self["tags"] = match_hans4.group(1) + "," + match_hans4.group(
                2) + match_hans4.group(3) + "," + match_hans4.group(4)
        elif match_hans3:
            self["tags"] = match_hans3.group(
                1) + "," + match_hans3.group(2) + "," + match_hans3.group(3)
        elif match_hans2:
            self["tags"] = match_hans2.group(
                1) + "," + match_hans2.group(2)
        elif match_hans1:
            self["tags"] = match_hans1.group(1)
        else:
            self["tags"] = ""
        params = (
            self["id"],
            self["name"],
            self["price"],
            self["address"],
            self["tags"],
            self["crawl_time"])

        return insert_sql, params


def remove_splash(value):
    # 去掉工作城市的斜线
    return value.replace("/", "")


def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)


class LagouJobItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


class LagouJobItem(scrapy.Item):
    # 拉勾网职位信息
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary_min = scrapy.Field()
    salary_max = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    work_years_min = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    work_years_max = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field(

    )
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_jobaddr),
    )
    company_name = scrapy.Field()
    company_url = scrapy.Field()
    tags = scrapy.Field(
        input_processor=Join(",")
    )
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into lagou_job(title, url, url_object_id, salary_min, salary_max, job_city, 
            work_years_min, work_years_max, degree_need, job_type, publish_time, job_advantage, 
            job_desc, job_addr, company_name, company_url, tags, crawl_time) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary_min=VALUES(salary_min), 
            salary_max=VALUES(salary_max), job_desc=VALUES(job_desc)
        """

        match_obj1 = re.match("经验(\d+)-(\d+)年", self['work_years_min'])
        match_obj2 = re.match("经验应届毕业生", self['work_years_min'])
        match_obj3 = re.match("经验不限", self['work_years_min'])
        match_obj4 = re.match("经验(\d+)年以下", self['work_years_min'])
        match_obj5 = re.match("经验(\d+)年以上", self['work_years_min'])

        if match_obj1:
            self['work_years_min'] = match_obj1.group(1)
            self['work_years_max'] = match_obj1.group(2)
        elif match_obj2:
            self['work_years_min'] = 0.5
            self['work_years_max'] = 0.5
        elif match_obj3:
            self['work_years_min'] = 0
            self['work_years_max'] = 0
        elif match_obj4:
            self['work_years_min'] = 0
            self['work_years_max'] = match_obj4.group(1)
        elif match_obj5:
            self['work_years_min'] = match_obj4.group(1)
            self['work_years_max'] = match_obj4.group(1) + 100
        else:
            self['work_years_min'] = 999
            self['work_years_max'] = 999

        match_salary = re.match("(\d+)[Kk]-(\d+)[Kk]", self['salary_min'])
        if match_salary:
            self['salary_min'] = match_salary.group(1)
            self['salary_max'] = match_salary.group(2)
        else:
            self['salary_min'] = 666
            self['salary_max'] = 666
        match_time1 = re.match("(\d+):(\d+).*", self["publish_time"])
        match_time2 = re.match("(\d+)天前.*", self["publish_time"])
        match_time3 = re.match("(\d+)-(\d+)-(\d+)", self["publish_time"])
        if match_time1:
            today = datetime.datetime.now()
            hour = int(match_time1.group(1))
            minutues = int(match_time1.group(2))
            time = datetime.datetime(today.year, today.month, today.day, hour, minutues)
            self["publish_time"] = time.strftime(SQL_DATETIME_FORMAT)
        elif match_time2:
            days_ago = int(match_time2.group(1))
            today = datetime.datetime.now() - datetime.timedelta(days=days_ago)
            self["publish_time"] = today.strftime(SQL_DATETIME_FORMAT)
        elif match_time3:
            year = int(match_time3.group(1))
            month = int(match_time3.group(2))
            day = int(match_time3.group(3))
            today = datetime.datetime(year, month, day)
            self["publish_time"] = today.strftime(SQL_DATETIME_FORMAT)
        else:
            self["publish_time"] = datetime.datetime.now(
            ).strftime(SQL_DATETIME_FORMAT)

        params = (
            self["title"],
            self["url"],
            self["url_object_id"],
            self["salary_min"],
            self["salary_max"],
            self["job_city"],
            self["work_years_min"],
            self["work_years_max"],
            self["degree_need"],
            self["job_type"],
            self["publish_time"],
            self["job_advantage"],
            self["job_desc"],
            self["job_addr"],
            self["company_name"],
            self["company_url"],
            self["tags"],
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params

    def save_to_es(self):
        lagou = LagouType()
        lagou.title = self['title']
        try:
            lagou.salary_min = int(self['salary_min'])
        except:
            pass
        try:
            lagou.salary_max = int(self['salary_max'])
        except:
            pass
        lagou.job_city = self['job_city']
        try:
            lagou.work_years_min = int(self['work_years_min'])
        except:
            pass
        try:
            lagou.work_years_max = int(self['work_years_max'])
        except:
            pass
        lagou.degree_need = self['degree_need']
        lagou.job_type = self['job_type']
        lagou.publish_time = self['publish_time'].replace('\xa0 ', '')
        lagou.job_advantage = self['job_advantage']
        lagou.job_addr = self['job_addr']
        lagou.job_desc = remove_tags(self['job_desc']).strip().replace("\r\n", "").replace("\t", "")
        lagou.company_name = self['company_name']
        lagou.company_url = self['company_url']
        try:
            lagou.tags = self['tags']
        except:
            pass
        lagou.crawl_time = self['crawl_time']
        lagou.url = self["url"]
        lagou.meta.id = self["url_object_id"]

        lagou.suggest = gen_suggests(LagouType._doc_type.index, ((lagou.title, 10), (lagou.tags, 7)))
        lagou.save()
        redis_cli.incr("job_count")

        return



