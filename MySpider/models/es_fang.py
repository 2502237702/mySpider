#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/4/6 17:13
# @Author  : LiuShaoheng


from elasticsearch_dsl import DocType, Date, Nested, Boolean, analyzer, InnerObjectWrapper, Completion, Keyword, Text, Integer
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer
from elasticsearch_dsl.connections import connections


# 与服务器进行连接，允许多个
connections.create_connection(hosts=["localhost"])


class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}


ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])


class FangType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    name = Text(analyzer="ik_max_word")
    price = Keyword()
    address = Keyword()
    tags = Text(analyzer="ik_max_word")
    crawl_time = Date()

    class Meta:
        index = "fang"
        doc_type = "fang"


if __name__ == "__main__":
    FangType.init()


















