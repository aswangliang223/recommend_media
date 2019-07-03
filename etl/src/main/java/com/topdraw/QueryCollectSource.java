package com.topdraw;


import com.topdraw.util.DruidDataSourceUt;
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

/**
 * @Author wangliang
 * @ClassName QueryCollectSource
 * @Description 用户历史收藏
 * @Date 2019/5/13
 **/
public class QueryCollectSource implements Job {

    private final static Logger logger = Logger.getLogger(QueryCollectSource.class);
    private final static String outputPath = AppConfiguration.get("mediaId_collect_path"); //hdfs存储路径
    private final static String sql = AppConfiguration.get("mediaId_collect_sql");

    @Override
    public void execute(JobExecutionContext context) {
        //配置HDFS
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://ns1");
        FSDataOutputStream fsDataOutputStream = null;
        BufferedWriter bwWriter = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            FileSystem fs = FileSystem.get(configuration);
            fsDataOutputStream = fs.create(new Path(outputPath));
            bwWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
            connection = DruidDataSourceUt.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String userId = "" + resultSet.getString("userId") + "";
                String mediaId = "" + resultSet.getString("mediaId") + "";
                String content = "{\"userId\":\"" + userId + "\",\"mediaId\":\"" + mediaId + "\"}";
                bwWriter.write(content);
                bwWriter.newLine();

            }
        } catch (Exception e) {
            logger.error("QueryMediaMessage exception");
            e.printStackTrace();
        } finally {
            try {
                if (bwWriter != null) {
                    bwWriter.close();
                }
                DruidDataSourceUt.closeResource(resultSet, preparedStatement, connection);
                if (fsDataOutputStream != null) {
                    fsDataOutputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("QueryMediaMessage stream close exception");
            }
        }
    }
}
