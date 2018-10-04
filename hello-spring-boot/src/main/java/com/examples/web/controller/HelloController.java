package com.examples.web.controller;

import org.apache.tomcat.util.threads.TaskQueue;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


@RestController
public class HelloController {

//    private static ThreadPoolExecutor pool = new ThreadPoolExecutor(5, 6,
//            60L, TimeUnit.SECONDS,
//            new LinkedBlockingQueue<Runnable>(5));
//    private static ThreadPoolExecutor pool;
//
//    static{
//
//        TaskQueue taskQueue = new TaskQueue();
//        pool = new ThreadPoolExecutor(3, 5,
//                60L, TimeUnit.SECONDS,  taskQueue);
//        taskQueue.setParent(pool);
//    }

    @RequestMapping("/hello")
    public String index() {
//        try {
//            TimeUnit.MINUTES.sleep(60);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        Runnable r = () -> {
//            System.out.println("rtest....");
//            try {
//                TimeUnit.MINUTES.sleep(60);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        };
//        pool.execute(r);
//        System.out.println("the task count  of pools is "+ pool.getTaskCount());
//        System.out.println("the size of pools is "+ pool.getPoolSize());
//        System.out.println("the max  size of pools is "+ pool.getMaximumPoolSize());
//        System.out.println("the queue of pools is "+ pool.getQueue().size());
        return "Hello World";
    }

}
