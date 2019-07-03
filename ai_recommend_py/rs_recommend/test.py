# -*- coding:utf-8 -*-
__author__ = 'wangliang'
from rs_recommend.readProperties import ReadProperties
from rs_recommend.loggingUtil import LoggingUtil


class Test:
    def __init__(self):
        self.file_path = "data/app.properties"

    def read(self):
        prop = ReadProperties.parse(self.file_path)
        print(prop.get("common_artist_tag_index_path"))
        logger = LoggingUtil("data/logs/")
        logger.log().error("1233")


if __name__ == '__main__':
    test = Test()
    test.read()
