# _*_ coding: utf-8 _*_


from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean,  analyzer, InnerObjectWrapper, Completion, Keyword, Text, Integer
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer
from elasticsearch_dsl.connections import connections


connections.create_connection(hosts=["localhost"])


class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}


ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])


class ZhiHuQuestionType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    # 知乎的问题 item
    zhihu_id = Keyword()
    topics = Text(analyzer="ik_max_word")
    url = Keyword()
    title = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")
    answer_num = Integer()
    comments_num = Integer()
    watch_user_num = Integer()
    click_num = Integer()
    crawl_time = Date()

    class Meta:
        index = "zhihu"
        doc_type = "question"


class ZhiHuAnswerType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    # 知乎的回答 item
    zhihu_id = Keyword()
    url = Keyword()
    question_id = Keyword()
    author_id = Keyword()
    content = Text(analyzer="ik_max_word")
    praise_num = Integer()
    comments_num = Integer()
    create_time = Date()
    update_time = Date()
    crawl_time = Date()
    author_name = Keyword()

    class Meta:
        index = "zhihu"
        doc_type = "answer"


if __name__ == "__main__":
    ZhiHuQuestionType.init()
    ZhiHuAnswerType.init()



