# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import numpy as np
from sklearn.cluster import AffinityPropagation
import os
import time
from ai_recommend_py.rs_recommend.loggingUtil import LoggingUtil
from ai_recommend_py.rs_recommend.readProperties import ReadProperties


class UserLabelAP:
    def __init__(self, media_ap_centers, media_ap_labels, media_ap_indices, mediaList_ap_centers, mediaList_ap_labels,
                 mediaList_ap_indices):
        self.media_ap_labels = media_ap_labels
        self.media_ap_centers = media_ap_centers
        self.mediaList_ap_centers = mediaList_ap_centers
        self.mediaList_ap_labels = mediaList_ap_labels
        self.media_ap_indices = media_ap_indices
        self.mediaList_ap_indices = mediaList_ap_indices
        self.logger = LoggingUtil("data/logs/")

    def isExists(self, fileName):
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

    def affinityPropagation(self, data, ap_damping, ap_max_iter, ap_convergence_iter, ap_copy, ap_preference,
                            ap_affinity, ap_verbose):
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
        af = AffinityPropagation(damping=ap_damping, max_iter=ap_max_iter, convergence_iter=ap_convergence_iter,
                                 copy=ap_copy, preference=ap_preference,
                                 affinity=ap_affinity, verbose=ap_verbose).fit(
            data)  # damping must be >= 0.5 and < 1,默认0.5
        tim_now = time.time()
        duration = tim_now - tim_pre
        self.logger.log().info(duration)
        cluster_centers_ = af.cluster_centers_
        labels_ = af.labels_
        cluster_centers_indices = af.cluster_centers_indices_
        return cluster_centers_, labels_, cluster_centers_indices

    def AP_algrothm(self, array):
        """
        将用户标签矩阵聚类,得到用户标签的聚类中心
        :param array:
        :return:cluster_centers_, labels_, cluster_centers_indices
        """
        prop = ReadProperties("data/app.properties")
        cluster_centers_, labels_, cluster_centers_indices = self.affinityPropagation(data=array,
                                                                                      ap_damping=float(
                                                                                          prop.get("ap_damping")),
                                                                                      ap_max_iter=int(prop.get(
                                                                                          "ap_max_iter")),
                                                                                      ap_convergence_iter=int(prop.get(
                                                                                          "ap_convergence_iter")),
                                                                                      ap_copy=True,
                                                                                      ap_preference=None,
                                                                                      ap_affinity=prop.get(
                                                                                          "ap_affinity"),
                                                                                      ap_verbose=False)
        return cluster_centers_, labels_, cluster_centers_indices

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
        # 单曲推荐的聚类中心
        cluster_centers_, labels_, cluster_centers_indices = self.AP_algrothm(array)
        self.logger.log().info("ap cluster centers nums %s" % cluster_centers_.shape[0])
        np.save(self.media_ap_centers, cluster_centers_)
        np.save(self.media_ap_labels, labels_)
        np.save(self.media_ap_indices, cluster_centers_indices)
        return cluster_centers_, labels_, cluster_centers_indices

    def run_Ap_songList_algrothm(self):
        """
        歌单专题的AP聚类
        :return:
        """
        prop = ReadProperties("data/app.properties")
        # 用户Index
        user_index_file = prop.get("userId_index_path")
        # （common）标签
        tag_common_file = prop.get("commonTag_index_path")
        # 歌单专题推荐不包含艺术家的用户标签打分
        user_common_tag_file = prop.get("user_common_tag_score_path")
        user_list = []
        tag_common_list = []
        user_list = self.file_read_fun(user_index_file, user_list)
        tag_common_list = self.file_read_fun(tag_common_file, tag_common_list)
        array = self.user_tag_matrix_fun(user_list, tag_common_list, user_common_tag_file)
        # 歌单聚类中心
        self.logger.log().info("song_list ap start...")
        cluster_centers_, labels_, cluster_centers_indices = self.AP_algrothm(array)
        self.logger.log().info("song_list ap finished!!!")
        self.logger.log().info("ap cluster centers nums: %s " % cluster_centers_.shape[0])
        self.logger.log().info("ap finished!!!")
        np.save(self.mediaList_ap_centers, cluster_centers_)
        np.save(self.mediaList_ap_labels, labels_)
        np.save(self.mediaList_ap_indices, cluster_centers_indices)
        # """
        # 绘制散点图观察聚类效果
        # """
        # import matplotlib.pyplot as plt
        # from itertools import cycle
        # plt.figure('AP')
        # plt.subplots(facecolor=(0.5, 0.5, 0.5))
        # colors = cycle('rgbcmykw')
        # for k, col in zip(range(cluster_centers_.shape[0]), colors):
        #     # labels == k 使用k与labels数组中的每个值进行比较
        #     # 如labels = [1,0],k=0,则‘labels==k’的结果为[False, True]
        #     class_members = labels_ == k
        #     cluster_center = array[cluster_centers_indices[k]]  # 聚类中心的坐标
        #     plt.plot(array[class_members, 0], array[class_members, 1], col + '.')
        #     plt.plot(cluster_center[0], cluster_center[1], markerfacecolor=col,
        #              markeredgecolor='k', markersize=14)
        #     for x in array[class_members]:
        #         plt.plot([cluster_center[0], x[0]], [cluster_center[1], x[1]], col)
        # plt.xticks(fontsize=10, color="darkorange")
        # plt.yticks(fontsize=10, color="darkorange")
        # plt.show()
        return cluster_centers_, labels_, cluster_centers_indices


if __name__ == "__main__":
    prop = ReadProperties("data/app.properties")
    userTagAp = UserLabelAP(prop.get("media_ap_centers"), prop.get("media_ap_labels"), prop.get("media_ap_indices"),
                            prop.get("media_list_ap_centers"), prop.get("media_list_ap_labels"),
                            prop.get("media_list_ap_indices"))
    userTagAp.run_Ap_songList_algrothm()
    userTagAp.run_Ap_single_algrothm()
