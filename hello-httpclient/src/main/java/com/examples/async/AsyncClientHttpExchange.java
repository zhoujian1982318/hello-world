package com.examples.async;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class AsyncClientHttpExchange {
    private static Logger LOG = LoggerFactory.getLogger(AsyncClientHttpExchange.class);
    public static void main(final String[] args) throws Exception {
        CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        try {
            httpclient.start();
            HttpGet request = new HttpGet("http://httpbin.org/get");
            Future<HttpResponse> future = httpclient.execute(request, null);
            HttpResponse response = future.get();
            LOG.info("the Response is {} ",  response.getStatusLine());
            LOG.info("shutting down");
        } finally {
            httpclient.close();
        }
        LOG.info("Done");
    }
}
