# -*- coding:utf-8 -*-
__author__ = 'wangliang'
from rs_recommend.loggingUtil import LoggingUtil
from rs_recommend.readProperties import ReadProperties
import numpy as np
from sklearn.cluster import KMeans, AffinityPropagation
import os
import time


class MediaLabelKMeans:
    def __init__(self, media_relation_ap_centers, media_relation_ap_labels):
        self.media_relation_ap_centers = media_relation_ap_centers
        self.media_relation_ap_labels = media_relation_ap_labels
        self.logger = LoggingUtil("data/logs/")
        self.prop = ReadProperties("data/app.properties")

    def isExists(self, file_name):
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
        if self.isExists(file_name):
            f = open(file_name, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                data.append(row[1].rstrip("\n"))
            f.close()
        else:
            self.logger.log().error("%s is not exists" % file_name)
        return data

    def media_tag_matrix_fun(self, media_list, tag_list, media_tag_relation_file):
        """
        user_tag_matrix_fun(返回歌曲标签矩阵，value为score)
        :param media_list:
        :param tag_list:
        :param media_tag_relation_file:
        :return: media_tag_mat
        """

        self.logger.log().info("media tag matrix building...")
        len_row = len(media_list)
        len_column = len(tag_list)
        array = np.zeros((len_row, len_column))
        if self.isExists(media_tag_relation_file):
            f = open(media_tag_relation_file, encoding="utf-8")
            for line in f.readlines():
                row = line.split("\t")
                media_id = row[0].strip()
                tag = row[1].strip()
                if tag in tag_list:
                    array[media_list.index(media_id)][tag_list.index(tag)] = 1.0
        self.logger.log().info("media tag matrix finished!!!")
        return array

    def k_Means_cluster_func(self, data):
        """
        cluster_centers_indices = af.cluster_centers_indices_
        label预测出来的每一个标签的类别标签，label是一个numpy数组
        cluster_centers_indices 预测出来的中心点的索引
        cluster_centers_ 聚类中心矩阵
        :param data:
        :return: cluster_centers_,labels_
        """
        tim_pre = time.time()
        self.logger.log().info(tim_pre)
        """
        damping : 衰减系数，默认为 0.5
        convergence_iter : 迭代次后聚类中心没有变化，算法结束，默认为15.
        max_iter : 最大迭代次数，默认200.
        copy : 是否在元数据上进行计算，默认True，在复制后的数据上进行计算。
        preference : S的对角线上的值
        affinity :S矩阵（相似度），默认为euclidean（欧氏距离）矩阵，即对传入的X计算距离矩阵，也可以设置为precomputed，那么X就作为相似度矩阵。
        verbose : 打印信息的详细程度，默认0，不打印
        """
        # af = AffinityPropagation(damping=0.9, max_iter=200, convergence_iter=15, copy=True, preference=None,
        #                          affinity='euclidean', verbose=False).fit(
        #     data)  # damping must be >= 0.5 and < 1,默认0.5
        k_means = KMeans(init='k-means++', n_clusters=50, n_init=10, max_iter=300)
        arrayk = k_means.fit(data)
        tim_now = time.time()
        duration = tim_now - tim_pre
        self.logger.log().info("the k-Means cluster used time %s" % duration)
        cluster_centers_ = arrayk.cluster_centers_
        labels_ = arrayk.labels_
        cluster_centers_indices = arrayk.cluster_centers_indices_
        return cluster_centers_, labels_, cluster_centers_indices

    def k_Means_algrothm(self, array):
        """
        将用户标签矩阵聚类,得到用户标签的聚类中心
        :param array:
        :return:cluster_centers_, labels_, cluster_centers_indices
        """
        prop = ReadProperties("data/app.properties")
        cluster_centers_, labels_, cluster_centers_indices = self.k_Means_cluster_func(data=array)
        return cluster_centers_, labels_, cluster_centers_indices

    def run_media_tag_algrothm(self):
        """
        单曲的AP聚类
        :return: cluster_centers_, labels_,cluster_centers_indices
        """

        prop = ReadProperties("data/app.properties")
        # 用户Index
        media_index_file = prop.get("all_mediaId_index_path")
        # （commonTag artistTag）标签
        tag_index_file = prop.get("commonTag_index_path")
        # 单曲推荐包含艺术家的用户标签评分
        media_tag_relation_file = prop.get("mediaId_common_tag_path")
        media_list = []
        tag_list = []
        media_list = self.file_read_fun(media_index_file, media_list)
        # tag_list 包含所有的标签（commonTag artistTag）：用于单曲推荐
        tag_list = self.file_read_fun(tag_index_file, tag_list)
        # array 为单曲推荐的矩阵（commonTag artistTag）
        array = self.media_tag_matrix_fun(media_list, tag_list, media_tag_relation_file)
        # media tag score ap聚类
        cluster_centers_, labels_, cluster_centers_indices = self.k_Means_algrothm(array)
        self.logger.log().info("ap cluster centers nums %s" % cluster_centers_.shape[0])
        np.save(self.media_relation_ap_centers, cluster_centers_)
        np.save(self.media_relation_ap_labels, labels_)
        return cluster_centers_, labels_, cluster_centers_indices


if __name__ == "__main__":
    prop = ReadProperties("data/app.properties")
    mediaTagAP = MediaLabelAP(prop.get("media_relation_ap_centers"), prop.get("media_relation_ap_labels"))
    mediaTagAP.run_media_tag_algrothm()
