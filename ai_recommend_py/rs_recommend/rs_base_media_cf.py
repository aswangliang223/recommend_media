# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import os
import numpy as np
from rs_recommend.readProperties import ReadProperties
from rs_recommend.loggingUtil import LoggingUtil
import math
from rs_recommend.similarity_matrix import Similarity_Matrix


class ItemBaseMediaCF:
    def __init__(self):
        self.prop = ReadProperties("data/app.properties")
        self.logger = LoggingUtil("data/logs/")
        pass

    def is_exist(self, fileName):
        """
        判断文件是否存在
        :param fileName:
        :return:boolean 是否存在
        """

        return os.path.exists(fileName)

    def file_read_fun(self, fileName, data):
        """
        读取数据格式为 (String String) 的 .csv文件
        :param fileName:
        :param data:
        :return:
        """
        if self.is_exist(fileName):
            f = open(fileName, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                data.append(row[1].rstrip("\n"))
            f.close()
        else:
            self.logger.log().error("%s is not exists" % fileName)
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
        :return:similarity_mat 相似度矩阵
        """
        sim_mat = Similarity_Matrix()
        len_row = array.shape[0]
        len_column = array.shape[0]
        similarity_mat = np.zeros((len_row, len_column))
        self.logger.log().info("cal similarity_mat start...")
        for i in range(len_row):
            for j in range(len_column):
                if i == j:
                    continue
                else:
                    score = math.fabs(sim_mat.similarity_func(array[i], array[j]))
                    similarity_mat[i][j] = score
            self.logger.log().info("cal part_%s" % (i + 1))
        self.logger.log().info("cal similarity_mat finished!!!")
        return similarity_mat

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
        # tag_list 包含所有的标签（commonTag artistTag）：用于单曲推荐
        tag_list = self.file_read_fun(tag_index_file, tag_list)
        # array 为单曲推荐的矩阵（commonTag artistTag）
        array = self.media_tag_matrix_fun(medaiId_list, tag_list, mediaId_tag_file)
        sim_media_mat = self.cal_similarity_func(array=array)
        np.savetxt(self.prop.get("media_relation_matrix_path"), sim_media_mat)


if __name__ == '__main__':
    itemBaseMediaCF = ItemBaseMediaCF()
    itemBaseMediaCF.main()
