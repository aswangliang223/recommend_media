package com.topdraw.listener;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

@WebListener
public class ServiceListener implements ServletContextListener {
	private static Logger logger;

	public static Scheduler scheduler = null;
	public static Boolean bStarted = true;

	public void contextDestroyed(ServletContextEvent event) {
		logger.debug("contextDestroyed");
		if (null != scheduler) try {
//				scheduler.pauseAll();
			bStarted = false;
			scheduler.shutdown(true);
		} catch (SchedulerException e) {
			logger.error("scheduler shutdown error", e);
		}
	}


	public void contextInitialized(ServletContextEvent event) {
		try {
			SchedulerFactory sf = new StdSchedulerFactory("quartz.properties");
			scheduler = sf.getScheduler();
			scheduler.start();
		} catch (Exception e) {
			logger.error("start quartz error", e);
		}

	}
}
