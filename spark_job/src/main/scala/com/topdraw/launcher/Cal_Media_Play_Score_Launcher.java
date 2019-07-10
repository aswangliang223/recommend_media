package com.topdraw.launcher;


import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;


/**
 * @Author WangLiang
 * @ClassName LauncherJob01
 * @Description spark launcher for cal_media_play_score
 * @Date 2019/5/24
 **/
public class Cal_Media_Play_Score_Launcher implements Job {
    private static  Logger logger =  Logger.getLogger(Cal_Media_Play_Score_Launcher.class);

    @Override
    public void execute(JobExecutionContext context) {
        HashMap env = new HashMap();
        //这两个属性必须设置
        env.put("HADOOP_CONF_DIR", "/x/app/hadoop-3.1.2/etc/hadoop/");
        env.put("JAVA_HOME", "/usr/java/jdk1.8.0_141-cloudera");
        //可以不设置
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        //env.put("YARN_CONF_DIR","");
        try {
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            //这里调用setJavaHome()方法后，JAVA_HOME is not set 错误依然存在
            SparkAppHandle handle = new SparkLauncher(env)
                    .setSparkHome("/x/app/spark-2.4.0-bin-hadoop2.7/")
                    .setAppResource("/x/data/ai_recommend/CalMediaScore.jar")
                    .setMainClass("com.topdraw.job.Cal_Media_Play_Score")
                    .setMaster("yarn")
                    .setAppName("CalMediaPlayScore_" + sdf.format(new Date()))
                    .setDeployMode("cluster")
                    .setConf("spark.driver.memory", "2g")
                    .setConf("spark.akka.frameSize", "200")
                    .setConf("spark.executor.memory", "1g")
                    .setConf("spark.executor.instances", "32")
                    .setConf("spark.executor.cores", "3")
                    .setConf("spark.default.parallelism", "10")
                    .setConf("spark.driver.allowMultipleContexts", "true")
                    .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
                        //这里监听任务状态，当任务结束时（不管是什么原因结束）,isFinal（）方法会返回true,否则返回false
                        public void stateChanged(SparkAppHandle sparkAppHandle) {
                            if (sparkAppHandle.getState().isFinal()) {
                                countDownLatch.countDown();
                            }
                            logger.info("state:" + sparkAppHandle.getState().toString());
                        }

                        public void infoChanged(SparkAppHandle sparkAppHandle) {
                            logger.info("Info:" + sparkAppHandle.getState().toString());
                        }
                    });
            logger.info("The task is executing, please wait ....");
            //线程等待任务结束
            countDownLatch.await();
            logger.info("The task is finished!");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage() + "==============================================");
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e.getMessage() + "==============================================");
        }
    }
}








