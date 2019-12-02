#!/x/install/PREFIX=/x/app/anaconda3/bin/python
# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import numpy as np
import os
from ai_recommend_py.rs_recommend.readProperties import ReadProperties
from ai_recommend_py.rs_recommend.loggingUtil import LoggingUtil
import pymysql
import datetime


class UserCenterTagFn(object):
    def __init__(self):
        self.prop = ReadProperties("data/app.properties")
        self.logger = LoggingUtil("data/logs/")
        self.user_tag_dict = {}
        pass

    def main(self):
        # media_ap_indices 返回的是一个聚类中心的索引 可以得到用户画像的标签信息
        user_media_indices_file = self.prop.get("media_ap_indices") + ".npy"
        userCenter_user_relation = np.load(user_media_indices_file)
        content = ""
        user_id_index_list = []
        userCenter_id_list = []
        # 查找用户表数据得到用户id，之后再去查找用户标签评分数据得到用户的标签，之后就得到用户聚类中心和标签的关系
        for i in range(0, userCenter_user_relation.shape[0]):
            user_id_index_list.append(userCenter_user_relation[i])
            userCenter_id_list.append(i)
        user_file = self.prop.get("user_now_play_index_path")
        # 用户list
        user_list = []
        user_list = self.file_read_fun_list(fileName=user_file, data=user_list)
        # 用户标签list
        user_tag_list = {}
        user_tag_list = self.file_read_fun_dict(fileName=self.prop.get("user_play_tag_score_single_mat_path"),
                                                data=user_tag_list)

        # 根据用户index 得到用户id
        userId_list = []
        for i in range(0, len(user_id_index_list)):
            userId_list.append(user_list[user_id_index_list[i]])
        user_tag_dict = {}
        for i in range(0, len(userId_list)):
            user_tag_dict.setdefault(i, {})
            user_tag_dict[i].setdefault(userId_list[i], [])
            if userId_list[i] in user_tag_list.keys():
                user_tag_dict[i][userId_list[i]].append(user_tag_list[userId_list[i]])
        self.user_tag_dict = user_tag_dict

    # 存入mysql
    def saveToSql(self):
        conn = None
        cur = None

        try:
            conn = pymysql.connect(host=self.prop.get("db_host"), port=int(self.prop.get("db_port")),
                                   db=self.prop.get("db"), user=self.prop.get("user_name"),
                                   password=self.prop.get("password"),
                                   charset='utf8')
            cur = conn.cursor()  # 获取游标
            cur.execute("TRUNCATE x_media__user_userCenter_tag")
            insert_sql = "INSERT INTO x_media__user_userCenter_tag (user_center_id,user_id,tag,score,create_time,update_time) VALUES"
            temp_sql = ""
            for i in range(0, len(self.user_tag_dict)):
                if str(self.user_tag_dict[i].values()).replace('dict_values', '').replace("(", "").replace(")",
                                                                                                           "").replace(
                    "[", "").replace("]", "") != "":
                    user_id, tags = self.dict_key_value(self.user_tag_dict[i])
                    keys, values = self.dict_key_value(eval(tags))
                    for j in range(0, len(str(keys).split(","))):
                        tagName = str(keys.split(",")[j])
                        score = float(values.split(",")[j])
                        userId = int(user_id)
                        userCenterId = i
                        temp_sql += "(%d,%d,\'%s\',%f,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'),str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" % (
                            userCenterId, userId, tagName, score,
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
            insert_sql += temp_sql
            conn.ping(reconnect=True)
            cur.execute(insert_sql.rstrip(','))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)
            self.logger.log().error(" insert error message : %s" % e)
        finally:
            self.logger.log().info("close streaming finished!!!")
            if conn is not None:
                conn.close()
            if cur is not None:
                cur.close()

    def file_read_fun_dict(self, fileName, data):
        """
        读取数据格式为 (String String) 的 .csv文件
        :param fileName:
        :param data:
        :return:
        """
        if os.path.exists(fileName):
            data = {}
            f = open(fileName, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                data.setdefault(row[0], {})
                data[row[0]].setdefault(row[1], 0)
                data[row[0]][row[1]] += float(row[2].strip("\n"))
            f.close()
        else:
            self.logger.log().error("%s is not exists" % fileName)
        return data

    def file_read_fun_list(self, fileName, data):
        """
        读取数据格式为 (String String) 的 .csv文件
        :param fileName:
        :param data:
        :return:
        """
        if os.path.exists(fileName):
            f = open(fileName, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                data.append(row[1].rstrip("\n"))
            f.close()
        else:
            self.logger.log().error("%s is not exists" % fileName)
        return data

    def dict_key_value(self, data):
        k = str(data.keys()).replace("dict_keys", "").replace("(", "").replace(")", "").replace("[", "").replace("]",
                                                                                                                 "").replace(
            "'", '')
        v = str(data.values()).replace("dict_values", "").replace("(", "").replace(")", "").replace("[", "").replace(
            "]", "")
        return k, v


if __name__ == '__main__':
    uctf = UserCenterTagFn()
    uctf.main()
    uctf.saveToSql()
