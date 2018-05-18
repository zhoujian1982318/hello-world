package com.examples.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class LongTask implements Callable<String> {

    private static Logger LOG = LoggerFactory.getLogger(LongTask.class);

    @Override
    public String call() throws Exception {
        LOG.info("run the long  periods task");
        LOG.info("long  periods task run int the thread is {}", Thread.currentThread().getName());
        TimeUnit.MINUTES.sleep(3);
        return "success";
    }
}
