# -*- coding:utf-8 -*-
__author__ = 'wangliang'
import hdfs as pyhdfs
import os
import datetime
from datetime import datetime
from ai_recommend_py.rs_recommend.loggingUtil import LoggingUtil
from ai_recommend_py.rs_recommend.readProperties import ReadProperties
import sys


class DownLoadData:
    def __init__(self, host, user_name):
        self.host = host
        self.user_name = user_name
        self.logger = LoggingUtil("data/logs/")

    def file_upload(self, local_path, hdfs_path):
        self.logger.log().info("file upload start...")
        fs = pyhdfs.HdfsClient(hosts=self.host, user_name=self.user_name)
        print(fs.listdir('/'))
        fs.copy_from_local(local_path, hdfs_path)
        self.logger.log().info("file upload finished !!!")

    def file_down(self, local_path, hdfs_path):
        self.logger.log().info("file download start")
        fs = pyhdfs.HdfsClient(hosts=self.host, user_name=self.user_name)
        # 判断目录是否存在 并且路径是否为目录
        try:
            if fs.exists(hdfs_path):
                if fs.get_file_status(path=hdfs_path).get("type") == "DIRECTORY":
                    for file in fs.listdir(hdfs_path):
                        if file.startswith("part"):
                            fs.copy_to_local(hdfs_path + "/" + file, local_path, overwrite=True)
                            self.logger.log().info(
                                "file download finished the fileName is %s" % (hdfs_path + "/" + file))
                    self.logger.log().info("down load file nums is %s" % len(fs.listdir(hdfs_path)))
                else:
                    self.logger.log().info("file download finished ther fileName is %s" % hdfs_path)
                    fs.copy_to_local(hdfs_path, local_path, overwrite=True)
            else:
                self.logger.log().error("filepath :  %s is not exists!!!" % hdfs_path)
                pass
        except Exception as e:
            print(e, file=sys.stderr)
            self.logger.log().error("fileNotFound Exception")

    def makeDir(self, path):
        is_exists = os.path.exists(path)
        # 判断结果
        if not is_exists:
            # 如果不存在则创建
            os.makedirs(path)
            # LoggingUtil.log().info(% path
            # "%s is make success")
            return True
        else:
            return False

    def main(self):
        prop_path = "data/app.properties"
        prop = ReadProperties(prop_path)
        date_path = datetime.today().strftime("%Y%m%d")
        # 创建一个日志文件夹
        self.makeDir("data/logs/")
        self.makeDir("data/input/")
        self.makeDir("data/output")
        # tagIndex信息 commonTag artistTag
        self.file_down(
            local_path=prop.get("common_artist_tag_index_path"),
            hdfs_path=prop.get("fs_common_artist_tag_index") + date_path)
        # 用户数据
        self.file_down(
            local_path=prop.get("userId_index_path"), hdfs_path=prop.get("fs_user_index") + date_path)
        # 歌单标签数据
        self.file_down(
            local_path=prop.get("mediaList_tag_path"),
            hdfs_path=prop.get("fs_media_list_tag"))
        # 专题标签数据
        self.file_down(
            local_path=prop.get("subject_tag_path"),
            hdfs_path=prop.get("fs_subject_tag"))

        # commonTagIndex 数据
        self.file_down(
            local_path=prop.get("commonTag_index_path"),
            hdfs_path=prop.get("fs_common_tag_index"))

        # 用户基础标签评分（歌单和专题推荐）
        self.file_down(
            local_path=prop.get("user_common_tag_score_path"),
            hdfs_path=prop.get("fs_user_common_tag_score") + date_path)

        # 用户标签评分(单曲推荐)
        self.file_down(
            local_path=prop.get("user_common_artist_tag_score_path"),
            hdfs_path=prop.get("fs_user_common_artist_tag_score") + date_path)

        # 歌单 index 数据（index mediaList_id）
        self.file_down(
            local_path=prop.get("mediaList_index_path"),
            hdfs_path=prop.get("fs_media_list_index"))

        # 单曲播放得分 和 用户之间的关系(userId,mediaId,score)
        self.file_down(
            local_path=prop.get("media_play_score_path"),
            hdfs_path=prop.get("fs_user_play_score") + date_path + "/")
        # 专题index
        self.file_down(
            local_path=prop.get("subject_index_path"),
            hdfs_path=prop.get("fs_subject_index"))

        # mediaId index
        self.file_down(local_path=prop.get("mediaId_index_path"), hdfs_path=prop.get("fs_mediaId_index") + date_path)
        #
        # mediaId tag
        self.file_down(local_path=prop.get("mediaId_tag_path"), hdfs_path=prop.get("fs_mediaId_tag") + date_path)

        # userId tag(commonTag) score 单纯的播放的用户标签关系 歌单专题推荐
        self.file_down(local_path=prop.get("user_play_tag_score_path"),
                       hdfs_path=prop.get("fs_user_play_tag_score_path") + date_path)

        # userId tag(commonTag) score 单纯的收藏的用户标签关系 歌单专题推荐
        self.file_down(local_path=prop.get("user_collect_tag_score_path"),
                       hdfs_path=prop.get("fs_user_collect_tag_score_path") + date_path)

        # userId tag(commonTag) score(disperse) 单纯的播放的用户标签离散化得分 歌单专题推荐
        self.file_down(local_path=prop.get("user_play_tag_disperse_score_path"),
                       hdfs_path=prop.get("fs_user_play_tag_disperse_score_mat_list_subject_path") + date_path)

        # index userId (now) 当前用户index 不包含历史收藏
        self.file_down(local_path=prop.get("user_now_play_index_path"),
                       hdfs_path=prop.get("fs_user_now_play_index_path") + date_path)

        # userId tag(common & artist tag) score 单纯播放的用户标签评分 单曲推荐
        self.file_down(local_path=prop.get("user_play_tag_score_single_mat_path"),
                       hdfs_path=prop.get("fs_user_play_tag_score_single_mat_path") + date_path)

        # 所有歌曲 index mediaId
        self.file_down(local_path=prop.get("all_mediaId_index_path"),
                       hdfs_path=prop.get("fs_all_mediaId_index_path"))

        # all mediaId common_tag
        self.file_down(local_path=prop.get("mediaId_common_tag_path"),
                       hdfs_path=prop.get("fs_mediaId_common_tag_path"))
        # one month media play score data
        self.file_down(local_path=prop.get("media_play_score_one_month_path"),
                       hdfs_path=prop.get("fs_media_play_score_one_month_path") + date_path)


if __name__ == '__main__':
    prop = ReadProperties("data/app.properties")
    loadData = DownLoadData(host=prop.get("host"), user_name=prop.get("user_name"))
    loadData.main()
