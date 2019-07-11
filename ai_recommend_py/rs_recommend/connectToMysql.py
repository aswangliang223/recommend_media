# -*- coding:utf-8 -*-
__author__ = 'wangliang'

import numpy as np
import datetime
import pymysql
import os
from rs_recommend.loggingUtil import LoggingUtil
from rs_recommend.readProperties import ReadProperties


class ConnectToMysql:
    def __init__(self):
        self.prop = ReadProperties("data/app.properties")
        self.logger = LoggingUtil("data/logs/")
        self.host = self.prop.get("db_host")
        self.port = self.prop.get("db_port")
        self.db = self.prop.get("db")
        self.user_name = self.prop.get("user_name")
        self.password = self.prop.get("password")

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

    def saveUserCenterToMysql(self, truncate_sql, user_id_list_file, user_labels_file, insert_sql):
        """
        将用户和用户聚类中心关系数据存入数据库
        :return:
        """
        conn = None
        cur = None
        try:
            conn = pymysql.connect(host=self.host, port=int(self.port), db=self.db, user=self.user_name,
                                   password=self.password,
                                   charset='utf8')
            self.logger.log().info("connect to mysql success !!!")
            # 将用户 和 用户中心存入 数据库 用户(user_list)和用户中心(labels) len 相同
            user_id_list = []
            user_id_list = self.file_read_fun(user_id_list_file, user_id_list)
            labels = np.load(user_labels_file)
            nums = len(labels)
            cur = conn.cursor()  # 获取游标
            cur.execute(truncate_sql)
            self.logger.log().info("truncate success table !!!")
            """2000个数据一组插入mysql中"""
            if nums > 2000:
                index = int(nums / 2000)
                for j in range(0, index):
                    sql = insert_sql
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
                sql = insert_sql
                for i in range(0, nums):
                    sql += "(%d,%d,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'),str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" % (
                        int(user_id_list[i]), labels[i],
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                conn.ping(reconnect=True)
                cur.execute(sql.rstrip(','))
                conn.commit()
            self.logger.log().info("insert  success !!!")
        except Exception as e:
            conn.rollback()
            print(e)
            self.logger.log().error(" insert error message : %s" % e)
        finally:
            self.logger.log().info("stream closed")
            conn.close()
            cur.close()

    def saveSimilarityMatrixToMysql(self, truncate_sql, similiarity_mat, media_file, insert_sql):
        """
        将相似度矩阵数据存入mysql
        :return:
        """
        conn = None
        cur = None
        try:
            conn = pymysql.connect(host=self.host, port=int(self.port), db=self.db, user=self.user_name,
                                   password=self.password,
                                   charset='utf8')
            self.logger.log().info("connect to mysql success !!!")
            """将 相似度计算后的 用户聚类中心和 歌单 的 socre 数据存入 mysql"""
            similiarity_matrix = np.loadtxt(similiarity_mat)
            media_list_fileName = media_file
            media_list_id = []
            media_list_id = self.file_read_fun(media_list_fileName, media_list_id)
            cur = conn.cursor()  # 获取游标
            cur.execute(truncate_sql)
            for i in range(0, similiarity_matrix.shape[0]):
                sql = insert_sql
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
        except Exception as e:
            conn.rollback()
            print(e)
            self.logger.log().error("error message:%s" % e)
        finally:
            self.logger.log().info("stream closed")
            conn.close()
            cur.close()

    def insertMediaRelationMatToMysql(self, truncate_sql, similarite_mat, insert_sql):
        conn = None
        cur = None
        media_relation_similarity_mat = np.loadtxt(similarite_mat)
        try:
            conn = pymysql.connect(host=self.host, port=int(self.port), db=self.db, user=self.user_name,
                                   password=self.password,
                                   charset='utf8')
            self.logger.log().info("connect to Mysql success!!!")
            cur = conn.cursor()  # 获取游标
            cur.execute(truncate_sql)
            for i in range(0, media_relation_similarity_mat.shape[0]):
                sql = insert_sql
                temp_sql = ""
                for j in range(0, media_relation_similarity_mat.shape[1]):
                    if i == j:
                        continue
                    else:
                        temp_sql += "(%s,%s,%f,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')," \
                                    "str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" \
                                    % (i + 1, j + 1, media_relation_similarity_mat[i][j],
                                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                        sql += temp_sql
            conn.ping(reconnect=True)
            cur.execute(sql.rstrip(','))
            conn.commit()
            self.logger.log().info("insert success part_%s !!!" % (i + 1))
        except Exception as e:
            conn.rollback()
            self.logger.log().error("error message:%s" % e)
        finally:
            self.logger.log().info("stream closed")
            conn.close()
            cur.close()

    def main(self):

        user_id_list_file = self.prop.get("user_now_play_index_path")
        user_mediaListSubject_labels_file = self.prop.get("media_list_ap_labels") + ".npy"
        # 单曲
        truncate_media = "TRUNCATE `x_user__user_center`"
        insert_media_sql = "insert into x_user__user_center(user_id,user_center_id,create_time,update_time) values "
        user_media_labels_file = self.prop.get("media_ap_labels") + ".npy"
        truncate_mat_media = "TRUNCATE `x_user_center__rank`"
        similiarity_mat_file = self.prop.get("media_similarity_matrix")
        media_mat_file = self.prop.get("all_mediaId_index_path")
        insert_mat_media_sql = "insert into x_user_center__rank(user_center_id,media_id,score,create_time,update_time) values "
        self.saveUserCenterToMysql(truncate_sql=truncate_media, user_id_list_file=user_id_list_file,
                                   user_labels_file=user_media_labels_file, insert_sql=insert_media_sql)
        self.saveSimilarityMatrixToMysql(truncate_sql=truncate_mat_media, similiarity_mat=similiarity_mat_file,
                                         media_file=media_mat_file, insert_sql=insert_mat_media_sql)

        # 歌单
        truncate_mediaList = "TRUNCATE `x_user__user_center_list`"
        insert_mediaList_sql = "insert into x_user__user_center_list(user_id,user_center_id,create_time,update_time) values "

        truncate_mat_mediaList = "TRUNCATE `x_user_center__rank_list`"
        similiarity_mat_file_mediaList = self.prop.get("media_list_similarity_matrix")
        mediaList_mat_file = self.prop.get("mediaList_index_path")
        insert_mat_mediaList_sql = "insert into x_user_center__rank_list(user_center_id,media_list_id,score,create_time,update_time) values "
        self.saveUserCenterToMysql(truncate_sql=truncate_mediaList, user_id_list_file=user_id_list_file,
                                   user_labels_file=user_mediaListSubject_labels_file, insert_sql=insert_mediaList_sql)
        self.saveSimilarityMatrixToMysql(truncate_sql=truncate_mat_mediaList,
                                         similiarity_mat=similiarity_mat_file_mediaList,
                                         media_file=mediaList_mat_file, insert_sql=insert_mat_mediaList_sql)
        # 专题
        truncate_mediaSubject = "TRUNCATE `x_user__user_center_subject`"
        insert_mediaSubject_sql = "insert into x_user__user_center_subject(user_id,user_center_id,create_time,update_time) values "

        truncate_mat_mediaSubject = "TRUNCATE `x_user_center__rank_subject`"
        similiarity_mat_file_mediaSubject = self.prop.get("media_subject_similarity_matrix")
        mediaSubject_mat_file = self.prop.get("subject_index_path")
        insert_mat_mediaSubject_sql = "insert into x_user_center__rank_subject(user_center_id,subject_id,score,create_time,update_time) values "
        self.saveUserCenterToMysql(truncate_sql=truncate_mediaSubject, user_id_list_file=user_id_list_file,
                                   user_labels_file=user_mediaListSubject_labels_file,
                                   insert_sql=insert_mediaSubject_sql)
        self.saveSimilarityMatrixToMysql(truncate_sql=truncate_mat_mediaSubject,
                                         similiarity_mat=similiarity_mat_file_mediaSubject,
                                         media_file=mediaSubject_mat_file, insert_sql=insert_mat_mediaSubject_sql)

        # 歌曲相似度计算 同类型推荐
        truncate_media_relation_sql = "TRUNCATE `x_cf_base_media_recommend`";
        insert_media_relation_sql = "INSERT INTO `x_cf_base_media_recommend`(mediaId_1,mediaId_2,score,create_time,update_time) VALUES "
        media_relation_similarity_mat_file = self.prop.get("media_relation_matrix_path")
        self.insertMediaRelationMatToMysql(truncate_sql=truncate_media_relation_sql,
                                           similarite_mat=media_relation_similarity_mat_file,
                                           insert_sql=insert_media_relation_sql)


if __name__ == "__main__":
    con = ConnectToMysql()
    con.main()
