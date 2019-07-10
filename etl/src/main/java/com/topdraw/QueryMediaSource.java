package com.topdraw;


import com.topdraw.util.DruidDataSourceUtil;
import org.afflatus.utility.AppConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.quartz.*;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wangliang
 * @date 2019-06-27-17:27
 * 获取元数据
 */
public class QueryMediaSource implements Job {

    private final static Logger logger = Logger.getLogger(QueryMediaSource.class);
    //index common_tag & artist_tag
    private final static String sql4 = AppConfiguration.get("artist_common_tag_index_sql");
    // index common_tag
    private final static String sql5 = AppConfiguration.get("common_tag_index_sql");
    // media_list tag
    private final static String sql2 = AppConfiguration.get("media_list_tag_sql");
    // subject_listId tagName
    private final static String sql3 = AppConfiguration.get("media_subject_tag_sql");
    //  index media_list
    private final static String sql6 = AppConfiguration.get("media_list_index_sql");
    //  index media_subject
    private final static String sql7 = AppConfiguration.get("media_subject_index_sql");
    //mediaId commonTag
    private final static String sql8 = AppConfiguration.get("mediaId_common_tag_sql");
    // mediaId artist_tag
    private final static String sql9 = AppConfiguration.get("mediaId_artist_tag_sql");
    // index mediaId
    private final static String sql10 = AppConfiguration.get("mediaId_index_sql");



    /**
     * 查询数据库获取歌曲标签数
     */
    public static void query(String sql, String outputPath) {

        //配置HDFS
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://ns1");
        FSDataOutputStream fsDataOutputStream = null;
        BufferedWriter bwWriter = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        //配置输出流
        try {
            FileSystem fs = FileSystem.get(configuration);
            //输出流
            fsDataOutputStream = fs.create(new Path(outputPath));
            bwWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
            connection = DruidDataSourceUtil.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String content = resultSet.getString("mid") + "\t" + resultSet.getString("tag");
                bwWriter.write(content);
                bwWriter.newLine();
            }
        } catch (Exception e) {
            logger.error("QueryMediaSource exception");
            e.printStackTrace();
        } finally {
            try {
                if (bwWriter != null) {
                    bwWriter.close();
                }
                DruidDataSourceUtil.closeResource(resultSet, preparedStatement, connection);
                if (fsDataOutputStream != null) {
                    fsDataOutputStream.close();
                }
            } catch (IOException e) {
                logger.error("QueryMediaSource stream close exception");
            }
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String nowTime = sdf.format(new Date());
        logger.info(nowTime + "==============================QueryMediaSource Job Start===========================");
        String mediaId_common_tag_path = AppConfiguration.get("mediaId_common_tag_path");
        String common_tag_index_path = AppConfiguration.get("common_tag_index_path");
        String mediaList_index_path = AppConfiguration.get("mediaList_index_path");
        String mediaList_tag_path = AppConfiguration.get("mediaList_tag_path");
        String mediaId_artist_tag_path = AppConfiguration.get("mediaId_artist_tag_path");
        String common_artist_tag_index_path = AppConfiguration.get("common_artist_tag_index_path");
        String subject_tag_path = AppConfiguration.get("subject_tag_path");
        String subject_index_path = AppConfiguration.get("subject_index_path");
        String mediaId_index_path = AppConfiguration.get("mediaId_index_path");
        query(sql2, mediaList_tag_path);
        query(sql5, common_tag_index_path);
        query(sql6, mediaList_index_path);
        query(sql8, mediaId_common_tag_path);
        query(sql9, mediaId_artist_tag_path);
        query(sql4, common_artist_tag_index_path);
        query(sql3, subject_tag_path);
        query(sql7, subject_index_path);
        query(sql10,mediaId_index_path);
        logger.info(nowTime + "==================================QueryMediaSource Job finished===================");
    }


    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String nowTime = sdf.format(new Date());
        logger.info(nowTime + "==============================QueryMediaSource Job Start===========================");
        String mediaId_common_tag_path = AppConfiguration.get("mediaId_common_tag_path");
        String common_tag_index_path = AppConfiguration.get("common_tag_index_path");
        String mediaList_index_path = AppConfiguration.get("mediaList_index_path");
        String mediaList_tag_path = AppConfiguration.get("mediaList_tag_path");
        String mediaId_artist_tag_path = AppConfiguration.get("mediaId_artist_tag_path");
        String common_artist_tag_index_path = AppConfiguration.get("common_artist_tag_index_path");
        String subject_tag_path = AppConfiguration.get("subject_tag_path");
        String subject_index_path = AppConfiguration.get("subject_index_path");
        String mediaId_index_path = AppConfiguration.get("mediaId_index_path");
        query(sql2, mediaList_tag_path);
        query(sql5, common_tag_index_path);
        query(sql6, mediaList_index_path);
        query(sql8, mediaId_common_tag_path);
        query(sql9, mediaId_artist_tag_path);
        query(sql4, common_artist_tag_index_path);
        query(sql3, subject_tag_path);
        query(sql7, subject_index_path);
        query(sql10,mediaId_index_path);
        logger.info(nowTime + "==================================QueryMediaSource Job finished===================");
    }
}
