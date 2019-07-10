# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import math
import numpy as np
import os
from rs_recommend.loggingUtil import LoggingUtil
from rs_recommend.readProperties import ReadProperties


class Similarity_Matrix:

    def __init__(self):
        self.logger = LoggingUtil("data/logs/")

    def similarity_func(self, A, B):
        """
        计算每个点与其他所有点之间的欧几里德距离
        :param A:
        :param B:
        :return:
        """
        # 这里是余弦相似度
        X = np.array(A)
        Y = np.array(B)
        AB = np.sum(X * Y)
        A2 = np.sqrt(np.sum(X * X))
        B2 = np.sqrt(np.sum(Y * Y))
        if A2 == 0 or B2 == 0:
            return 0
        else:
            return AB / ((A2 * B2))

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

    def cluster_media_list_fun(self, cluster_centers, array):
        """
        # 计算相似度
        :param cluster_centers:
        :param array:
        :return: similarityMatrix
        """
        len_row = cluster_centers.shape[0]
        len_coloum = array.shape[0]
        similarityMatrix = np.zeros((len_row, len_coloum))
        for i in range(len_row):
            for j in range(len_coloum):
                score = math.fabs(self.similarity_func(cluster_centers[i], array[j]))
                similarityMatrix[i][j] = score
        return similarityMatrix

    def generate_relation_mat(self, row, column, file):
        """
        建立歌单和标签的矩阵 行:歌单,列:标签
        :param mediaList_list:
        :param tag_list:
        :param mediaList_fileName:
        :return: array
        """
        len_row = len(row)
        len_column = len(column)
        array = np.zeros((len_row, len_column))
        if self.isExists(file):
            f = open(file, encoding="utf-8")
            for line in f.readlines():
                arr = line.split("\t")
                media = arr[0]
                tag = arr[1].rstrip("\n")
                if tag in column:
                    array[row.index(media)][column.index(tag)] = 1.0
        return array

    def cal_similarity_func(self, row_file, column_file, row_coloum_relation_file, cluster_centers, mat_out_file):
        """
        similarity_media_list_func 计算相似度
        :return: similarity_matrix_media_list
        """
        row_index = []
        column_index = []
        self.logger.log().info("read row_index_file start...")
        row_index = self.file_read_fun(row_file, row_index)
        self.logger.log().info("read row_index_file finished!!!")
        self.logger.log().info("read column_index_file start...")
        column_index = self.file_read_fun(column_file, column_index)
        self.logger.log().info("read column_index_file finished!!!")
        array = self.generate_relation_mat(row_index, column_index, row_coloum_relation_file)
        self.logger.log().info("calculate similarity matrix start...")
        similarity_matrix_media = self.cluster_media_list_fun(cluster_centers, array)
        self.logger.log().info("calculate similarity matrix finished !!!")
        np.savetxt(mat_out_file, similarity_matrix_media)
        return similarity_matrix_media


if __name__ == '__main__':
    sim_mat = Similarity_Matrix()
    prop = ReadProperties("data/app.properties")
    # 单曲相似度计算(应该是全部歌曲) #todo：
    sim_mat.cal_similarity_func(row_file=prop.get("all_mediaId_index_path"),
                                column_file=prop.get("common_artist_tag_index_path"),
                                row_coloum_relation_file=prop.get("mediaId_tag_path"),
                                cluster_centers=np.load(prop.get("media_ap_centers") + ".npy"),
                                mat_out_file=prop.get("media_similarity_matrix"))
    # 歌单相似度计算
    sim_mat.cal_similarity_func(row_file=prop.get("mediaList_index_path"), column_file=prop.get("commonTag_index_path"),
                                row_coloum_relation_file=prop.get("mediaList_tag_path"),
                                cluster_centers=np.load(prop.get("media_list_ap_centers") + ".npy"),
                                mat_out_file=prop.get("media_list_similarity_matrix"))

    # 专题相似度计算
    sim_mat.cal_similarity_func(row_file=prop.get("subject_index_path"), column_file=prop.get("commonTag_index_path"),
                                row_coloum_relation_file=prop.get("subject_tag_path"),
                                cluster_centers=np.load(prop.get("media_list_ap_centers") + ".npy"),
                                mat_out_file=prop.get("media_subject_similarity_matrix"))
