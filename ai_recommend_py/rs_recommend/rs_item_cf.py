#!/usr/bin/python
# coding:utf8
"""
协同过滤的方式（基于user 的推荐 用户 歌曲 播放次数（=1））
"""
from __future__ import print_function
import math
import random
from operator import itemgetter

import pymysql

from rs_recommend.loggingUtil import LoggingUtil
from rs_recommend.readProperties import ReadProperties
import datetime

# 使得随机数据可预测
random.seed(0)


class ItemBasedCF():
    """
    TopN recommendation - ItemBasedCF
    """

    def __init__(self):
        self.prop = ReadProperties("data/app.properties")
        self.logger = LoggingUtil("data/logs/")
        self.host = self.prop.get("db_host")
        self.port = int(self.prop.get("db_port"))
        self.db = self.prop.get("db")
        self.user = self.prop.get("user_name")
        self.password = self.prop.get("password")
        self.trainset = {}
        self.testset = {}

        # n_sim_user: top 20个用户， n_rec_media: top 10个推荐结果
        self.n_sim_media = 20
        self.n_rec_media = 10

        # media_sim_mat: 歌曲之间的相似度， media_popular: 歌曲出现的次数， media_count: 播放歌曲的总数据
        self.media_sim_mat = {}
        self.media_popular = {}
        self.media_count = 0
        self.rec_media_dict = {}
        self.all_rec_medias = {}

        self.logger.log().info('Similar media number = %d' % self.n_sim_media)
        self.logger.log().info('Recommended media number = %d' % self.n_rec_media)

    @staticmethod
    def loadfile(filename):
        """loadfile(加载文件，返回一个生成器)

        Args:
            filename   文件名
        Returns:
            line       行数据，去空格
        """
        fp = open(filename, 'r')
        for i, line in enumerate(fp):
            yield line.strip('\r\n')
            # if i > 0 and i % 100000 == 0:
            #     print('loading %s(%s)' % (filename, i), file=sys.stderr)
        fp.close()

    def generate_dataset(self, filename, pivot=0.7):
        """loadfile(加载文件，将数据集按照7:3 进行随机拆分)
        Args:
            filename   文件名
            pivot      拆分比例
        """
        trainset_len = 0
        testset_len = 0

        for line in self.loadfile(filename):
            user, media, rating, count, percentage_count = line.split('\t')
            # 通过pivot和随机函数比较，然后初始化用户和对应的值
            if random.random() < pivot:
                # dict.setdefault(key, default=None)
                # key -- 查找的键值
                # default -- 键不存在时，设置的默认键值
                self.trainset.setdefault(user, {})
                self.trainset[user][media] = str(rating + "\t" + count + "\t" + percentage_count)
                trainset_len += 1
            else:
                self.testset.setdefault(user, {})
                self.testset[user][media] = str(rating + "\t" + count + "\t" + percentage_count)
                testset_len += 1
        self.logger.log().info('分离训练集和测试集成功')
        self.logger.log().info('train set = %s' % trainset_len)
        self.logger.log().info('test set = %s' % testset_len)

    def calc_media_sim(self):
        """
        calc_media_sim()
        :return: item_sim_mat
        """
        self.logger.log().info('counting medias number and popularity...')
        # 统计在所有的用户中`，不同歌曲的总播放次数， user, medias 某首歌曲被多少人播放过，数据来源中用户播放一首歌曲的所有记录被评分在了一起
        for _, medias in self.trainset.items():
            for media in medias:
                # count item popularity
                if media not in self.media_popular:
                    self.media_popular[media] = 0
                self.media_popular[media] += 1

        self.logger.log().info('count medias number and popularity success')

        # total numbers of media
        self.media_count = len(self.media_popular)
        self.logger.log().info('total media number = %d' % self.media_count)

        # 统计在相同用户时，不同歌曲同时出现的次数（本意就是用户播放的每一首曲子与之相关的其他曲子的播放次数）
        item_sim_mat = self.media_sim_mat
        self.logger.log().info('building co-rated users matrix...')
        # user, medias
        for _, medias in self.trainset.items():
            for m1 in medias:
                for m2 in medias:
                    if m1 == m2:
                        continue
                    item_sim_mat.setdefault(m1, {})
                    item_sim_mat[m1].setdefault(m2, 0)
                    item_sim_mat[m1][m2] += 1
        self.logger.log().info('build co-rated users matrix success')

        # calculate similarity matrix
        self.logger.log().info('calculating media similarity matrix...')
        simfactor_count = 0
        for m1, related_movies in item_sim_mat.items():
            for m2, count in related_movies.items():
                # 余弦相似度
                item_sim_mat[m1][m2] = count / math.sqrt(
                    self.media_popular[m1] * self.media_popular[m2])
                simfactor_count += 1
        self.logger.log().info('calculate media similarity matrix(similarity factor) success')
        self.logger.log().info('total similarity factor number = %d' % simfactor_count)

    def recommend(self, user):
        """recommend(找出top K的歌曲，对歌曲进行相似度sum的排序，取出top N的歌曲)
        Args:
            user       用户
        Returns:
            rec_movie  歌曲推荐列表，按照相似度从大到小的排序
        """
        ''' Find K similar medias and recommend N medias. '''
        K = self.n_sim_media
        N = self.n_rec_media
        rank = {}
        listened_media = self.trainset[user]

        # rating=歌曲得分, w=不同歌曲出现的次数
        for media, rating in listened_media.items():
            if media in self.media_sim_mat.keys():
                for related_media, w in sorted(
                        self.media_sim_mat[media].items(),
                        key=itemgetter(1),
                        reverse=True):
                    if related_media in listened_media:
                        continue
                    rank.setdefault(related_media, 0)
                    rank[related_media] += w * float(str(rating).split("\t")[0])
        # return the N best medias
        return sorted(rank.items(), key=itemgetter(1), reverse=True)

    def evaluate(self):
        """
        :param self:
        :return: precision, recall, coverage and popularity
        """
        self.logger.log().info('Evaluation start...')

        # 返回top N的推荐结果
        N = self.n_rec_media
        # varables for precision and recall
        # hit表示命中(测试集和推荐集相同+1)，rec_count 每个用户的推荐数， test_count 每个用户对应的测试数据集的歌曲数目
        hit = 0
        rec_count = 0
        test_count = 0
        # varables for coverage
        # varables for popularity
        popular_sum = 0

        # enumerate将其组成一个索引序列，利用它可以同时获得索引和值
        # 参考地址：http://blog.csdn.net/churximi/article/details/51648388
        for i, user in enumerate(self.trainset):
            if i > 0 and i % 500 == 0:
                self.logger.log().info('recommended for %d users' % i)
            test_medias = self.testset.get(user, {})
            rec_medias = self.recommend(user)

            # 对比测试集和推荐集的差异 media, w
            for media, _ in rec_medias:
                if media in test_medias:
                    hit += 1
                self.all_rec_medias.setdefault(user, {})
                self.all_rec_medias[user][media] = float(_)
                # 计算用户对应的歌曲出现次数log值的sum加和
                popular_sum += math.log(1 + self.media_popular[str(media).split("\t")[0]])
            rec_count += N
            test_count += len(test_medias)

        precision = hit / (1.0 * rec_count)  # 命中/总推荐次数
        recall = hit / (1.0 * test_count)  # 命中/总测试数据
        coverage = len(self.all_rec_medias) / (1.0 * self.media_count)  # 推荐结果覆盖所有歌曲的覆盖率
        popularity = popular_sum / (1.0 * rec_count)  # 这个参数越大说明数据关联性越强

        self.logger.log().info('precision=%.4f \t recall=%.4f \t coverage=%.4f \t popularity=%.4f' % (
            precision, recall, coverage, popularity))

    def insert_to_mysql(self):
        """
        将based-item 的用户推荐结果插入数据库
        :return:
        """
        conn = None
        cur = None
        try:
            self.logger.log().info("connect to mysql start ....")
            conn = pymysql.connect(host=self.host, port=self.port, db=self.db, user=self.user,
                                   password=self.password,
                                   charset='utf8')
            self.logger.log().info("connect to mysql success !!!")

            cur = conn.cursor()  # 获取游标
            cur.execute('truncate  x_cf_item_recommend')
            self.logger.log().info("truncate table:x_cf_item_recommend success !!!")

            count = 0
            sql = "insert into x_cf_item_recommend(user_id,media_id,score,create_time,update_time) values "
            temp_sql = ""
            total_count = 0
            for user in self.all_rec_medias:
                media_score = self.all_rec_medias[user]
                if len(media_score) >= 5:
                    total_count += 5
                else:
                    total_count += len(media_score)
            count_index = 0
            for i, user in enumerate(self.all_rec_medias):
                media_score = self.all_rec_medias[user]
                for j, media in enumerate(media_score):
                    if j < 5:
                        score = media_score[media]
                        temp_sql = "(\'%d\',\'%s\',%f,str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'),str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s'))" % (
                            int(user), str(media).split("\t")[0], float(score),
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ","
                        sql += temp_sql
                        count_index += 1
                        if (count_index % 2000 == 0) & (count_index != total_count):
                            conn.ping(reconnect=True)
                            cur.execute(sql.rstrip(","))
                            conn.commit()
                            sql = "insert into x_cf_item_recommend(user_id,media_id,score,create_time,update_time) values "
                            temp_sql = ""
                            self.logger.log().info("insert to mysql part-%s success !!!" % (int(count_index / 2000)))
                            continue
                        if count_index == total_count:
                            conn.ping(reconnect=True)
                            cur.execute(sql.rstrip(","))
                            conn.commit()
                    else:
                        break
        except Exception as e:
            self.logger.log().error("insert error %s" % e)
        finally:
            conn.close()
            cur.close()
            self.logger.log().info("close streaming...")


if __name__ == '__main__':
    # ratingfile = 'data/16.RecommenderSystems/ml-1m/ratings.dat'
    prop = ReadProperties("data/app.properties")
    ratingfile = prop.get("media_play_score_one_month_path")
    # 创建ItemCF对象
    itemcf = ItemBasedCF()
    # 将数据按照 7:3的比例，拆分成：训练集和测试集，存储在usercf的trainset和testset中
    itemcf.generate_dataset(ratingfile, pivot=0.7)
    # 计算用户之间的相似度
    itemcf.calc_media_sim()
    # 评估推荐效果
    itemcf.evaluate()
    # itemcf.insert_to_mysql()
    # 查看推荐结果用户
    user = "173955"
    print("推荐结果", itemcf.recommend(user))
    print("---", itemcf.testset.get(user, {}))
