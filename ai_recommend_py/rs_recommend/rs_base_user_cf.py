#!/x/install/PREFIX=/x/app/anaconda3/bin/python
# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import sys
import random
import math
import os
from operator import itemgetter

from collections import defaultdict
from ai_recommend_py.rs_recommend.dbUtil import BaseDao
import datetime

random.seed(0)


class UserBasedCF(object):
    """
    TopN recommendation - User Based Collaborative Filtering 
    """

    def __init__(self):
        self.trainset = {}
        self.testset = {}

        self.n_sim_user = 20
        self.n_rec_media = 10

        self.user_sim_mat = {}
        self.media_popular = {}
        self.media_count = 0

        self.all_rec_media = {}

        print('Similar user number = %d' % self.n_sim_user, file=sys.stderr)
        print('recommended media number = %d' %
              self.n_rec_media, file=sys.stderr)

    @staticmethod
    def loadfile(filename):
        """ load a file, return a generator. """
        fp = open(filename, 'r')
        for i, line in enumerate(fp):
            yield line.strip('\r\n')
            if i % 100000 == 0:
                print('loading %s(%s)' % (filename, i), file=sys.stderr)
        fp.close()
        print('load %s success' % filename, file=sys.stderr)

    def generate_dataset(self, filename, pivot=0.7):
        ''' load rating data and split it to training set and test set '''
        trainset_len = 0
        testset_len = 0

        for line in self.loadfile(filename):
            user, media, rating, play_count, play_rating = line.split('\t')
            # split the data by pivot
            if random.random() < pivot:
                self.trainset.setdefault(user, {})
                self.trainset[user][media] = float(rating)
                trainset_len += 1
            else:
                self.testset.setdefault(user, {})
                self.testset[user][media] = float(rating)
                testset_len += 1

        print('split training set and test set success', file=sys.stderr)
        print('train set = %s' % trainset_len, file=sys.stderr)
        print('test set = %s' % testset_len, file=sys.stderr)

    def calc_user_sim(self):
        ''' calculate user similarity matrix '''
        # build inverse table for item-users
        # key=movieID, value=list of userIDs who have seen this movie
        print('building media-users inverse table...', file=sys.stderr)
        media2users = dict()

        for user, medias in self.trainset.items():
            for media in medias:
                # inverse table for item-users
                if media not in media2users:
                    media2users[media] = set()
                media2users[media].add(user)
                # count item popularity at the same time
                if media not in self.media_popular:
                    self.media_popular[media] = 0
                self.media_popular[media] += 1
        print('build media-users inverse table success', file=sys.stderr)

        # save the total media number, which will be used in evaluation
        self.media_count = len(media2users)  # media count
        print('total media number = %d' % self.media_count, file=sys.stderr)

        # count co-rated items between users
        usersim_mat = self.user_sim_mat
        print('building user co-rated medias matrix...', file=sys.stderr)

        for media, users in media2users.items():
            for u in users:
                usersim_mat.setdefault(u, defaultdict(int))
                for v in users:
                    if u == v:
                        continue
                    usersim_mat[u][v] += 1
        print('build user co-rated media matrix success', file=sys.stderr)

        # calculate similarity matrix
        print('calculating user similarity matrix...', file=sys.stderr)
        simfactor_count = 0
        PRINT_STEP = 2000000

        for u, related_users in usersim_mat.items():
            for v, count in related_users.items():
                usersim_mat[u][v] = count / math.sqrt(
                    len(self.trainset[u]) * len(self.trainset[v]))
                simfactor_count += 1
                if simfactor_count % PRINT_STEP == 0:
                    print('calculating user similarity factor(%d)' %
                          simfactor_count, file=sys.stderr)

        print('calculate user similarity matrix(similarity factor) success',
              file=sys.stderr)
        print('Total similarity factor number = %d' %
              simfactor_count, file=sys.stderr)

    def recommend(self, user):
        """ Find K similar users and recommend N movies. """
        K = self.n_sim_user
        N = self.n_rec_media
        rank = dict()
        watched_movies = self.trainset[user]

        for similar_user, similarity_factor in sorted(self.user_sim_mat[user].items(),
                                                      key=itemgetter(1), reverse=True)[0:K]:
            for media in self.trainset[similar_user]:
                if media in watched_movies:
                    continue
                # predict the user's "interest" for each movie
                rank.setdefault(media, 0)
                rank[media] += similarity_factor
        # return the N best movies
        return sorted(rank.items(), key=itemgetter(1), reverse=True)[0:N]

    def evaluate(self):
        ''' print evaluation result: precision, recall, coverage and popularity '''
        print('Evaluation start...', file=sys.stderr)

        N = self.n_rec_media
        #  varables for precision and recall
        hit = 0
        rec_count = 0
        test_count = 0
        # varables for coverage
        all_rec_medias = set()
        # varables for popularity
        popular_sum = 0

        for i, user in enumerate(self.trainset):
            if i % 500 == 0:
                print('recommended for %d users' % i, file=sys.stderr)
            test_media = self.testset.get(user, {})
            rec_media = self.recommend(user)
            for media, _ in rec_media:
                if media in test_media:
                    hit += 1
                # 所有的推荐结果
                self.all_rec_media.setdefault(user, {})
                self.all_rec_media[user][media] = float(_)
                all_rec_medias.add(media)
                popular_sum += math.log(1 + self.media_popular[media])
            rec_count += N
            test_count += len(test_media)

        precision = hit / (1.0 * rec_count)
        recall = hit / (1.0 * test_count)
        coverage = len(all_rec_medias) / (1.0 * self.media_count)
        popularity = popular_sum / (1.0 * rec_count)

        print('precision=%.4f\trecall=%.4f\tcoverage=%.4f\tpopularity=%.4f' %
              (precision, recall, coverage, popularity), file=sys.stderr)

    def saveData(self):
        CONFIG = {
            "user": "root",
            "password": "Topdraw1qaz",
            "database": "ai_recommend",
            "table": "x_cf_user_recommend"
        }
        # 初始化指定的table
        baseDao = BaseDao(**CONFIG)

        # 清空数据库
        baseDao.truncate()

        # 插入数据库
        item = []
        for i, user in enumerate(self.all_rec_media):
            for media in self.all_rec_media[user]:
                temp_dict = {
                    'id': None,
                    'user_id': str(user),
                    'media_id': str(media),
                    'score': float(self.all_rec_media[user][media]),
                    'create_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'update_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                item.append(temp_dict)
        baseDao.batch_save(obj=item)


if __name__ == '__main__':
    ratingfile = os.path.join('data/input', 'media_play_one_month_score_data.csv')
    usercf = UserBasedCF()
    usercf.generate_dataset(ratingfile)
    usercf.calc_user_sim()
    usercf.evaluate()
    usercf.saveData()
    # user = "173955"
    # print("推荐结果", usercf.recommend(user))
    # print("---", usercf.testset.get(user, {}))
