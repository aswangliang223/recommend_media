# -*- coding:utf-8 -*-
__author__ = 'wangliang'

import numpy as np
import datetime
import pymysql
import os

from rs_recommend.loggingUtil import LoggingUtil


class ConnectToMysql:
    def __init__(self):
        pass

    def file_read_fun(self, fileName, data):
        """
        读取数据格式为 (String String) 的 .csv文件
        :param fileName:
        :param data:
        :return:
        """
        if self.isExists(fileName):
            f = open(fileName, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                data.append(row[1].rstrip("\n"))
            f.close()
        return data

    def isExists(self, fileName):
        """
        判断文件是否存在
        :param fileName:
        :return:
        """
        return os.path.exists(fileName)

    def saveUserCenterToMysql(self):
        """
        将用户和用户聚类中心关系数据存入数据库
        :return:
        """
        """
        conn = pymysql.connect(host='bigdatatopdraw003', port=3306, db="ai_recommend", user="ai", password="topdraw_ai",charset='utf8')
        """
        try:
            conn = pymysql.connect(host='bigdatatopdraw003', port=3306, db="ai_recommend", user="root",
                                   password="Tjlh@2017",
                                   charset='utf8')
            LoggingUtil.log().info("connect to mysql success !!!")
        except Exception as e:
            print(e)
            LoggingUtil.log().error("connect to mysql error,the error message is :%s" % e)
        # 将用户 和 用户中心存入 数据库 用户(user_list)和用户中心(labels) len 相同
        user_id_list = []
        user_id_list = self.file_read_fun("data/input/userId.csv", user_id_list)
        labels = np.load("data/output/mediaList_ap_labels.npy")
        nums = len(labels)
        try:
            cur = conn.cursor()  # 获取游标
            cur.execute('truncate  x_user__user_center')
            LoggingUtil.log().info("truncate table:x_user__user_center success !!!")
            """2000个数据一组插入mysql中"""
            if nums > 2000:
                index = int(nums / 2000)
                for j in range(0, index):
                    sql = "insert into x_user__user_center(user_id,user_center_id,create_time,update_time) values "
                    temp_sql = ""
                    count = 0
                    for i in range(count + j * 2000, (j + 1) * 2000):
                        temp_sql = "(%d,%d,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'),str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" % (
                            int(user_id_list[i]), labels[i],
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                        sql += temp_sql
                        """rstrip(",")表示删除字符串右边的, lstrip(",") 表示删除左边的"""
                        count += 1
                    if j + 1 == index:
                        for k in range(index * 2000, nums):
                            temp_sql = "(%d,%d,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'),str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" % (
                                int(user_id_list[k]), labels[k],
                                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                            sql += temp_sql
                    conn.ping(reconnect=True)
                    cur.execute(sql.rstrip(','))
                    conn.commit()
            else:
                sql = "insert into x_user__user_center(user_id,user_center_id,create_time,update_time) values "
                for i in range(0, nums):
                    sql += "(%d,%d,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'),str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" % (
                        int(user_id_list[i]), labels[i],
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                conn.ping(reconnect=True)
                cur.execute(sql.rstrip(','))
                conn.commit()
            cur.close()
            LoggingUtil.log().info("insert user__user_center_relation success !!!")
        except Exception as e:
            conn.rollback()
            print(e)
            LoggingUtil.log().error(" insert error message : %s" % e)
        finally:
            conn.close()

    def saveSimilarityMatrixToMysql(self):
        """
        将相似度矩阵数据存入mysql
        :return:
        """
        try:
            conn = pymysql.connect(host='bigdatatopdraw003', port=3306, db="ai_recommend", user="root",
                                   password="Tjlh@2017",
                                   charset='utf8')
            LoggingUtil.log().info("connect to mysql success !!!")
            """将 相似度计算后的 用户聚类中心和 歌单 的 socre 数据存入 mysql"""
        except Exception as e:
            print(e)
            LoggingUtil.log().error("connect to mysql error,the error messag: %s" % e)

        similiarity_matrix = np.loadtxt("data/output/media_list_similarity_matrix")
        media_list_fileName = "data/input/mediaList_index.csv"
        media_list_id = []
        media_list_id = self.file_read_fun(media_list_fileName, media_list_id)
        try:
            cur = conn.cursor()  # 获取游标
            cur.execute("truncate x_user_center__rank")
            for i in range(0, similiarity_matrix.shape[0]):
                sql = "insert into x_user_center__rank(user_center_id,media_list_id,score,create_time,update_time) values "
                temp_sql = ""
                for j in range(0, similiarity_matrix.shape[1]):
                    if similiarity_matrix[i][j] == 0:
                        continue
                    else:
                        temp_sql = "(%d,%d,%f,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')," \
                                   "str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" \
                                   % (i, int(media_list_id[j]), similiarity_matrix[i][j],
                                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                      datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                        sql += temp_sql
                if str(sql.rstrip()).endswith('values'):
                    continue
                conn.ping(reconnect=True)
                cur.execute(sql.rstrip(','))
                conn.commit()
            cur.close()
        except Exception as e:
            conn.rollback()
            print(e)
            LoggingUtil.log().error("error message:%s" % e)
        finally:
            conn.close()


if __name__ == "__main__":
    con = ConnectToMysql()
    con.saveSimilarityMatrixToMysql()
    con.saveUserCenterToMysql()
