package com.topdraw.listener;

import org.apache.log4j.Logger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 * @author wangliang
 * @date 2019-06-27-17:24
 * quartz 任务监听
 */
@WebListener
public class ServiceListener implements ServletContextListener {

    private static Logger logger;

    public static Scheduler scheduler = null;
    public static Boolean bStarted = true;

    public void contextInitialized(ServletContextEvent sce) {
        logger.debug("contextDestroyed");
        if (null != scheduler) {
            try {
                bStarted = false;
                scheduler.shutdown(true);
            } catch (SchedulerException e) {
                logger.error("scheduler shutdown error", e);
            }
        }
    }

    public void contextDestroyed(ServletContextEvent sce) {
        try {
            SchedulerFactory sf;
            sf = new StdSchedulerFactory("quartz.properties");
            scheduler = sf.getScheduler();
            scheduler.start();
        } catch (Exception e) {
            logger.error("start quartz error", e);
        }


    }
}
