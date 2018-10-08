package com.examples.demo;

import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QuickStart {


    public static class Task1 implements Runnable {

        private CloseableHttpClient httpclient;

        public Task1(CloseableHttpClient httpclient) {
            this.httpclient = httpclient;
        }

        @Override
        public void run() {

            try {
                HttpGet httpGet = new HttpGet("http://www.apache.org/");
                System.out.println("execute http get...");
                CloseableHttpResponse response1 = httpclient.execute(httpGet);
                // The underlying HTTP connection is still held by the response object
                // to allow the response content to be streamed directly from the network socket.
                // In order to ensure correct deallocation of system resources
                // the user MUST call CloseableHttpResponse#close() from a finally clause.
                // Please note that if response content is not fully consumed the underlying
                // connection cannot be safely re-used and will be shut down and discarded
                // by the connection manager.
                try {
                    System.out.println(response1.getStatusLine());
                    HttpEntity entity1 = response1.getEntity();
                    // do something useful with the response body
                    // and ensure it is fully consumed
                    EntityUtils.consume(entity1);
                } finally {
                    response1.close();
                }
                System.out.println("finish  http request...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Task2 implements Runnable {

        private CloseableHttpClient httpclient;

        public Task2(CloseableHttpClient httpclient) {
            this.httpclient = httpclient;
        }

        @Override
        public void run() {

            try {
                HttpGet httpGet = new HttpGet("http://www.apache.org/");
                System.out.println("execute http get...");
                CloseableHttpResponse response1 = httpclient.execute(httpGet);
                // The underlying HTTP connection is still held by the response object
                // to allow the response content to be streamed directly from the network socket.
                // In order to ensure correct deallocation of system resources
                // the user MUST call CloseableHttpResponse#close() from a finally clause.
                // Please note that if response content is not fully consumed the underlying
                // connection cannot be safely re-used and will be shut down and discarded
                // by the connection manager.
                try {
                    System.out.println(response1.getStatusLine());
                    HttpEntity entity1 = response1.getEntity();
                    // do something useful with the response body
                    // and ensure it is fully consumed
                    EntityUtils.consume(entity1);
                } finally {
                    response1.close();
                }
                System.out.println("finish  http request...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Task3 implements Runnable {

        private PoolingHttpClientConnectionManager cm;

        public Task3(PoolingHttpClientConnectionManager cm) {
            this.cm = cm;
        }

        @Override
        public void run() {
            try {
                while (true) {

                    TimeUnit.SECONDS.sleep(5);
                    // Close expired connections


                    System.out.println("the pool status is " + cm.getTotalStats());

                    System.out.println("check expired connection");

                    cm.closeExpiredConnections();

                    SimpleDateFormat sf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    System.out.println("the current time is : " + sf.format(new Date()));

                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //CloseableHttpClient httpclient = HttpClients.createDefault();
        RequestConfig config = RequestConfig.custom()
                //.setConnectTimeout(5000)
                .setConnectionRequestTimeout(5000)
                .build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(4);

        ConnectionKeepAliveStrategy myStrategy = new ConnectionKeepAliveStrategy() {

            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                System.out.println("getKeepAliveDuration ");
                // Honor 'keep-alive' header
                HeaderElementIterator it = new BasicHeaderElementIterator(
                        response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    System.out.println("the header param  is :" + param);
                    System.out.println("the header value  is :" + value);
                    if (value != null && param.equalsIgnoreCase("timeout")) {
                        try {
                            return Long.parseLong(value) * 1000;
                        } catch(NumberFormatException ignore) {
                        }
                    }
                }
                HttpHost target = (HttpHost) context.getAttribute(HttpClientContext.HTTP_TARGET_HOST);
                System.out.println("the target is :" + target);
                System.out.println("keep alive 5 second");
                return 5*1000;

            }

        };
        CloseableHttpClient httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                //.setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                .setKeepAliveStrategy(myStrategy)
                .setDefaultRequestConfig(config)
                .build();


//        try {
//            HttpGet httpGet = new HttpGet("http://httpbin.org/get");
//            System.out.println("execute http get...");
//            CloseableHttpResponse response1 = httpclient.execute(httpGet);
//            // The underlying HTTP connection is still held by the response object
//            // to allow the response content to be streamed directly from the network socket.
//            // In order to ensure correct deallocation of system resources
//            // the user MUST call CloseableHttpResponse#close() from a finally clause.
//            // Please note that if response content is not fully consumed the underlying
//            // connection cannot be safely re-used and will be shut down and discarded
//            // by the connection manager.
//            try {
//                System.out.println(response1.getStatusLine());
//                HttpEntity entity1 = response1.getEntity();
//                // do something useful with the response body
//                // and ensure it is fully consumed
//                EntityUtils.consume(entity1);
//            } finally {
//                response1.close();
//            }
//
//            HttpPost httpPost = new HttpPost("http://httpbin.org/post");
//            List<NameValuePair> nvps = new ArrayList<NameValuePair>();
//            nvps.add(new BasicNameValuePair("username", "vip"));
//            nvps.add(new BasicNameValuePair("password", "secret"));
//            httpPost.setEntity(new UrlEncodedFormEntity(nvps));
//            System.out.println("execute http post...");
//            CloseableHttpResponse response2 = httpclient.execute(httpPost);
//
//            try {
//                System.out.println(response2.getStatusLine());
//                HttpEntity entity2 = response2.getEntity();
//                // do something useful with the response body
//                // and ensure it is fully consumed
//                EntityUtils.consume(entity2);
//            } finally {
//                //response2.close();
//            }
//
//            TimeUnit.SECONDS.sleep(60);
//        } finally {
//            httpclient.close();
//        }

        ExecutorService excecutor = Executors.newCachedThreadPool();

        excecutor.submit(new Task1(httpclient));

        excecutor.submit(new Task2(httpclient));

        excecutor.submit(new Task3(cm));

        excecutor.awaitTermination(300, TimeUnit.SECONDS);
//        Thread t1 = new Thread(new Task1(httpclient));
//        t1.start();
//
//        Thread t2 = new Thread(new Task2(httpclient));
//        t2.start();


        //TimeUnit.SECONDS.sleep(300);

        //httpclient.close();
//      HttpClientContext context = HttpClientContext.create();
//      HttpClientConnectionManager connMrg = new BasicHttpClientConnectionManager();
//      HttpRoute route = new HttpRoute(new HttpHost("www.apache.org", 80));
//// Request new connection. This can be a long process
//        ConnectionRequest connRequest = connMrg.requestConnection(route, null);
//// Wait for connection up to 10 sec
//        HttpClientConnection conn = connRequest.get(10, TimeUnit.SECONDS);
//        try {
//            // If not open
//            if (!conn.isOpen()) {
//                System.out.println("connect www.apache.org");
//                // establish connection based on its route info
//                connMrg.connect(conn, route, 1000, context);
//                // and mark it as route complete
//                connMrg.routeComplete(conn, route, context);
//            }
//            // Do useful things with the connection.
//            TimeUnit.SECONDS.sleep(5);
//        } finally {
//            System.out.println("release connection");
//            connMrg.releaseConnection(conn, null, 30, TimeUnit.SECONDS);
//        }
//
//        TimeUnit.SECONDS.sleep(60);
//    }
    }
}