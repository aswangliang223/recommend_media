package com.topdraw.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.log4j.Logger;


import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;


/**
 * @Author wangliang
 * @ClassName DruidDataSourceUtil
 * @Description Druid连接
 * @Date 2019/4/28
 **/
public class DruidDataSourceUt {
    private final static Logger logger = Logger.getLogger(DruidDataSourceUt.class);
    //申明DataSource
    public static DataSource dataSource;

    //初始化连接池
    static {
        //加载配置文件
        InputStream is = DruidDataSourceUt.class.getClassLoader().getResourceAsStream("druid_media_collect_source_read_00.properties");
        //创建连接池对象
        try {
            Properties pro = new Properties();
            //加载io流
            pro.load(is);
            //使用第三方连接池druid
            dataSource = DruidDataSourceFactory.createDataSource(pro);
        } catch (Exception e) {
            logger.error("Druid connect error");
        }

    }

    /**
     * 获取连接池的方法
     *
     * @return
     */
    public static DataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取连接
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 关闭资源的方法
     *
     * @param resultSet
     * @param preparedStatement
     * @param connection
     */
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
        // 关闭结果集
        // ctrl+alt+m 将java语句抽取成方法
        closeResultSet(resultSet);
        // 关闭语句执行者
        closeStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    /**
     * 重载closeResource 方法
     *
     * @param preparedStatement
     * @param connection
     */
    public static void closeResource(PreparedStatement preparedStatement, Connection connection) {
        // 关闭语句执行者
        closeStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("connection close exception");
            }
        }
    }

    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error("resultSet close exception");
            }
        }
    }

    private static void closeStatement(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                logger.error("statement close exception");
            }
        }
    }

}