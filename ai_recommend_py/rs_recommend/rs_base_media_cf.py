# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import os
import numpy as np
from rs_recommend.readProperties import ReadProperties
from rs_recommend.loggingUtil import LoggingUtil
import math
from rs_recommend.similarity_matrix import Similarity_Matrix
import pymysql
import datetime


class ItemBaseMediaCF:
    def __init__(self):
        self.prop = ReadProperties("data/app.properties")
        self.logger = LoggingUtil("data/logs/")
        self.media_list = {}
        self.host = self.prop.get("db_host")
        self.port = self.prop.get("db_port")
        self.db = self.prop.get("db")
        self.user_name = self.prop.get("user_name")
        self.password = self.prop.get("password")
        pass

    def is_exist(self, file_name):
        """
        判断文件是否存在
        :param fileName:
        :return:boolean 是否存在
        """

        return os.path.exists(file_name)

    def file_read_fun(self, file_name, data):
        """
        读取数据格式为 (String String) 的 .csv文件
        :param fileName:
        :param data:
        :return:
        """
        if self.is_exist(file_name):
            f = open(file_name, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                data.append(row[1].rstrip("\n"))
            f.close()
        else:
            self.logger.log().error("%s is not exists" % file_name)
        return data

    def media_tag_matrix_fun(self, media_id_list, tag_list, media_tag_file_name):
        """
        user_tag_matrix_fun(返回歌曲标签矩阵，value为score)
        :param media_id_list:
        :param tag_list:
        :param media_tag_file_name:
        :return: 歌曲标签矩阵
        """

        self.logger.log().info("media tag matrix building...")
        len_row = len(media_id_list)
        len_column = len(tag_list)
        array = np.zeros((len_row, len_column))
        if self.is_exist(media_tag_file_name):
            f = open(media_tag_file_name, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                media_id = row[0].strip()
                tag = row[1].strip()
                if tag in tag_list:
                    array[media_id_list.index(media_id)][tag_list.index(tag)] = 1.0
        self.logger.log().info("media tag matrix finished!!!")
        return array

    def cal_similarity_func(self, array):
        """
        计算相似度
        :return:similarity_mat 相似度矩阵，top5存入mysql
        """
        sim_mat = Similarity_Matrix()
        len_row = array.shape[0]
        len_column = array.shape[0]
        self.logger.log().info("cal similarity_mat start...")
        conn = None
        cur = None

        try:
            conn = pymysql.connect(host=self.host, port=int(self.port), db=self.db, user=self.user_name,
                                   password=self.password,
                                   charset='utf8')
            self.logger.log().info("connect to Mysql success!!!")
            cur = conn.cursor()  # 获取游标
            # cur.execute("TRUNCATE `x_cf_base_media_recommend`")
            sql = "INSERT INTO `x_cf_base_media_recommend`(mediaId_1,mediaId_2,score,create_time,update_time) VALUES"
            temp_sql = ""
            for i in range(len_row):
                media = {}
                # 索引的关系
                media.setdefault(self.media_list[i], {})
                for j in range(len_column):
                    if i == j:
                        media[self.media_list[i]][self.media_list[j]] = 0.0
                        continue
                    else:
                        score = math.fabs(sim_mat.similarity_func(array[i], array[j]))
                        media[self.media_list[i]][self.media_list[j]] = float(score)
                # media 根据 得分排序取前十个
                media_popular = list(sorted(media[self.media_list[i]].items(), key=lambda x: x[1], reverse=True))[:10]
                media_id_1 = list(media.keys())[0]
                for k in range(len(media_popular)):
                    temp_sql = "(%d,%d,%f,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')," \
                               "str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" \
                               % (int(media_id_1), int(media_popular[k][0]), float(media_popular[k][1]),
                                  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                    sql += temp_sql
                if (i != 0) & ((i + 1) % 200 == 0) & ((i + 1) != len_row):
                    conn.ping(reconnect=True)
                    cur.execute(sql.rstrip(','))
                    conn.commit()
                    sql = "INSERT INTO `x_cf_base_media_recommend`(mediaId_1,mediaId_2,score,create_time,update_time) VALUES"
                    temp_sql = ""
                    self.logger.log().info("cal media cosine similarity %s" % str(i+1))
                else:
                    if (i + 1) == len_row:
                        conn.ping(reconnect=True)
                        cur.execute(sql.rstrip(','))
                        conn.commit()
                        self.logger.log().info("cal media cosine similarity completed !!!")
        except Exception as e:
            conn.rollback()
            self.logger.log().error("error message:%s" % e)
        finally:
            self.logger.log().info("stream closed")
            conn.close()
            cur.close()

    def main(self):
        """
        同类推荐歌曲相似度计算
        :return:
        """
        # media Index
        mediaId_index_file = self.prop.get("all_mediaId_index_path")
        # （commonTag）标签
        tag_index_file = self.prop.get("commonTag_index_path")
        # 单曲推荐包含艺术家的用户标签评分
        mediaId_tag_file = self.prop.get("mediaId_common_tag_path")
        medaiId_list = []
        tag_list = []
        medaiId_list = self.file_read_fun(mediaId_index_file, medaiId_list)
        self.media_list = medaiId_list
        # tag_list 包含所有的标签（commonTag artistTag）：用于单曲推荐
        tag_list = self.file_read_fun(tag_index_file, tag_list)
        # array 为单曲推荐的矩阵（commonTag artistTag）
        array = self.media_tag_matrix_fun(medaiId_list, tag_list, mediaId_tag_file)
        self.cal_similarity_func(array=array)


if __name__ == '__main__':
    itemBaseMediaCF = ItemBaseMediaCF()
    itemBaseMediaCF.main()
