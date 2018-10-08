package com.examples.low.client;


import com.examples.demo.pojo.ApCacheData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

public class RestClientTest {

    private RestClient restClient;

    private ObjectMapper objMaper;


    @Before
    public void setUp() throws Exception {
        restClient = RestClient.builder(new HttpHost("192.168.20.61", 9200, "http")).build();
        objMaper = new ObjectMapper();

    }

    @Test
    public void testPerformRequest() throws IOException {
        Response response = restClient.performRequest("GET", "/");

        String responseBody = EntityUtils.toString(response.getEntity());

        ReadContext ctx = JsonPath.parse(responseBody);

        assertEquals("neb-db-03",  ctx.read("$.name"));
    }

    @Test
    public  void addApCache() throws IOException, ParseException {

        Map<String,String> params = Collections.<String, String>emptyMap();



        String time = "2018-06-12 18:00:00";
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date  dt = sf.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dt);

//        for(int i=0;  i< 10 ; i++) {
//            ApCacheData apCache = new ApCacheData();
//            apCache.setAcctId(1);
//            apCache.setApName("R2AP-B482C5-000537");
//            apCache.setApMac("B4:82:C5:00:05:37");
//            apCache.setHits(10+i);
//            apCache.setMisses(15+i);
//            apCache.setTs(calendar.getTimeInMillis());
//            apCache.setStrTime(sf.format(calendar.getTime()));
//            calendar.add(Calendar.MINUTE, 10);
//            HttpEntity entity = new NStringEntity(objMaper.writeValueAsString(apCache), ContentType.APPLICATION_JSON);
//
//            Response response = restClient.performRequest("POST", "/cache/content/", params, entity);
//
//            String responseBody = EntityUtils.toString(response.getEntity());
//
//            ReadContext ctx = JsonPath.parse(responseBody);
//
//            assertEquals("created", ctx.read("$.result"));
//        }

        //RestHighLevelClient client
    }

}
