package com.topdraw;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @Author wangliang
 * @ClassName FTPUtil
 * @Description
 * @Date 2019/4/25
 **/
public class FTPUtil implements Job {
    private static Logger logger = Logger.getLogger(FTPUtil.class);
    public static String url = "47.100.131.226"; //ftp服务器地址
    public static String username = "topdraw"; //ftp登录账号
    public static String password = "Topdraw1qaz"; //ftp登录密码
    public static String rootPath = "/cmcc_gansu/logs/"; //ftp服务器上文件的相对地址
    private static String hdfsPath = "/ai/data/input"; //hdfs 上文件的保存位置

    /**
     * ftp文件下载
     *
     * @param url
     * @param username
     * @param password
     * @param rootPath
     * @param hdfsPath
     * @param configuration
     * @return
     */
    public static boolean ftpUploadFile(String url, String username, String password, String rootPath, String hdfsPath, Configuration configuration) {
        FTPClient ftpClient = new FTPClient();
        InputStream inputStream = null;
        FSDataOutputStream fsDataOutputStream = null;
        BufferedReader brLogFileReader = null;
        BufferedWriter bwLogFileWriter = null;
        boolean flage = true;
        String fileNamesStr = "";//记住上传的文件名称
        try {
            //连接ftp
            ftpClient.connect(url);
            ftpClient.login(username, password);
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            ftpClient.setControlEncoding("UTF-8");
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
            }
            //工作目录转到 ftp 目录下
            ftpClient.changeWorkingDirectory(rootPath);
            ftpClient.enterLocalPassiveMode();
            FTPFile[] ftpFiles = ftpClient.listFiles(rootPath);
            FileSystem fileSystem = FileSystem.get(configuration);
            //DateTimeFormat.ISO不安全兼容ISO 8601
            DateTimeFormatter parser = ISODateTimeFormat.dateTimeNoMillis();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (FTPFile file : ftpFiles) {
                String fileName = file.getName();
                if (!fileName.endsWith(".log")) { //只读取.log 文件
                    continue;
                }
                inputStream = ftpClient.retrieveFileStream(rootPath + fileName);
                if (inputStream != null) {
                    //读取文件内容
                    brLogFileReader = new BufferedReader(new InputStreamReader(inputStream));

                    //输出文件名称定义
                    String strFileName = fileName.substring(0, fileName.lastIndexOf("."));
                    String[] strFileNames = strFileName.split("_");
                    if (strFileNames.length != 5) {
                        logger.error("FTPUtil => ftpUploadFile FTP文件名称异常【" + fileName + "】");
                        continue;
                    }
                    String hdfsFileName = strFileNames[2] + "/" + fileName.substring(0, strFileName.length());

                    //输出流
                    fsDataOutputStream = fileSystem.create(new Path(hdfsPath + "/mediaPlay/" + hdfsFileName));
                    bwLogFileWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));

                    String strLine;
                    while ((strLine = brLogFileReader.readLine()) != null) {
                        String[] logInfo = strLine.split("\u0001");
                        Object json = JSONObject.parse(logInfo[1]);
                        if (json instanceof JSONArray) {
                            JSONArray array = (JSONArray) json;
                            if (array.size() > 0) {
                                String actionStr = array.getJSONObject(0).get("action").toString(); //获取操作类型
                                if (!"DataShow".equals(actionStr)) {
                                    for (int i = 0; i < array.size(); i++) {
                                        String userId = array.getJSONObject(i).get("userId").toString();
                                        String mediaId = array.getJSONObject(i).get("mediaId").toString();
                                        String actionTime = array.getJSONObject(i).get("actionTime").toString();
                                        int duration = Integer.valueOf(array.getJSONObject(i).get("duration").toString());
                                        String position = "-";
                                        String startTime = "-";
                                        String endTime = "-";
                                        if (!"MediaSeek".equals(actionStr)) {
                                            position = array.getJSONObject(i).get("position").toString();
                                        } else {
                                            startTime = array.getJSONObject(i).get("startTime").toString();
                                            endTime = array.getJSONObject(i).get("endTime").toString();
                                        }
                                        String content = actionStr + "\t" + userId + "\t" + mediaId + "\t" + position + "\t" + startTime + "\t" + endTime + "\t" + duration + "\t" + actionTime;
                                        bwLogFileWriter.write(content);
                                        bwLogFileWriter.newLine();
                                    }
                                }
                            }

                        }
                    }
                    bwLogFileWriter.flush();
                    //计算上传文件数
                    fileNamesStr += fileName + ",";

                    //关闭流
                    if (bwLogFileWriter != null) {
                        bwLogFileWriter.close();
                    }
                    if (fsDataOutputStream != null) {
                        fsDataOutputStream.close();
                    }
                    if (brLogFileReader != null) {
                        brLogFileReader.close();
                    }
                    if (inputStream != null) {
                        inputStream.close();
                    }
                    ftpClient.completePendingCommand(); //循环执行报错 必须加上
                    //读取完毕 hdfs 存在文件 就加 后缀
                    if (isExist(hdfsPath + "/mediaPlay/" + hdfsFileName)) {
                        ftpClient.changeWorkingDirectory(rootPath);
                        ftpClient.rename(fileName, fileName + ".complete");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            flage = false;
            logger.error("FTPUtil => ftpUploadFile error", e);
        } finally {
            try {
                //关闭流
                if (bwLogFileWriter != null) {
                    bwLogFileWriter.close();
                }
                if (fsDataOutputStream != null) {
                    fsDataOutputStream.close();
                }
                if (brLogFileReader != null) {
                    brLogFileReader.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                if (ftpClient != null) {
                    ftpClient.disconnect();
                }
            } catch (IOException e) {
                logger.error("FTPUtil => ftpUploadFile error", e);
            }
            logger.info("FTPUtil JOB 本次上传文件【" + fileNamesStr + "】");
        }

        return flage;
    }

    /**
     * @param jobExecutionContext
     * @throws JobExecutionException
     */
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String nowTime = sdf.format(new Date());
        logger.info(nowTime + "==============================FTPUpload Job Start===========================");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://ns1");
        if (!ftpUploadFile(url, username, password, rootPath, hdfsPath, configuration)) {
            logger.error("FTPUtil job do error");
        } else {
            logger.info("FTPUtil job do stop");
        }

    }

    /**
     * 判断文件是否存在hdfs
     *
     * @param filePath
     * @return
     */
    private static boolean isExist(String filePath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ns1");
        FileSystem fileSystem = FileSystem.get(conf);
        return fileSystem.exists(new Path(filePath));
    }
}
