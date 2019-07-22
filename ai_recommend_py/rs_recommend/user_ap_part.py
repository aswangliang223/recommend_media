#!/x/install/PREFIX=/x/app/anaconda3/bin/python
# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import numpy as np
import os
from rs_recommend.readProperties import ReadProperties
from rs_recommend.loggingUtil import LoggingUtil

"""
用户分段的ap聚类
"""


class UserAPPart():
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
        else:
            self.logger.log().error("%s is not exists" % fileName)
        return data

    def user_tag_matrix_fun(self, userId_list, tag_list, user_tag_fileName):
        """
        user_tag_matrix_fun(返回用户标签矩阵，value为score)
        :param userId_list:
        :param tag_list:
        :param user_tag_fileName:
        :return: 用户标签矩阵
        """
        self.logger.log().info("user tag matrix building...")
        len_row = len(userId_list)
        len_coloum = len(tag_list)
        array = np.zeros((len_row, len_coloum))
        if self.isExists(user_tag_fileName):
            f = open(user_tag_fileName, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                userId = row[0].strip()
                tag = row[1].strip()
                score = row[2].rstrip("\n")
                if tag in tag_list:
                    array[userId_list.index(userId)][tag_list.index(tag)] = score
        self.logger.log().info("user tag matrix finished!!!")
        return array

    def run_Ap_single_algrothm(self):
        """
        单曲的AP聚类
        :return: cluster_centers_, labels_,cluster_centers_indices
        """

        prop = ReadProperties("data/app.properties")
        # 用户Index
        user_index_file = prop.get("userId_index_path")
        # （commonTag artistTag）标签
        tag_index_file = prop.get("common_artist_tag_index_path")
        # 单曲推荐包含艺术家的用户标签评分
        user_tag_file1 = prop.get("user_common_artist_tag_score_path")
        user_list = []
        tag_list = []
        user_list = self.file_read_fun(user_index_file, user_list)
        # tag_list 包含所有的标签（commonTag artistTag）：用于单曲推荐
        tag_list = self.file_read_fun(tag_index_file, tag_list)
        # array 为单曲推荐的矩阵（commonTag artistTag）
        array = self.user_tag_matrix_fun(user_list, tag_list, user_tag_file1)

        # 做标签降维

        mark = 2000
        divide_array = int(array.shape[0] / mark)  # 分组数
        mod = array.shape[0] % mark


if __name__ == '__main__':
    pass
