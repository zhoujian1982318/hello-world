package com.examples.hig.client;


import com.examples.demo.pojo.ApCacheData;
import com.examples.demo.pojo.ContentCacheData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.*;

import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

public class HighRestClientTest {

    private static Logger LOG = LoggerFactory.getLogger(HighRestClientTest.class);

    private RestHighLevelClient client;


    private ObjectMapper objMaper;


    @Before
    public void setUp() throws Exception {

        //默认NHttpClientConnectionManager pool MaxtTotal 的最大连接数是 30， DefaultMaxPerRoute 为 10， 如果要配置 调用
        //setRequestConfigCallback (httpClientConfigCallback  )
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("10.22.0.52", 9200, "http")));
        //new HttpHost("10.22.0.67",9200, "http"),
        //new HttpHost("10.22.0.59",9200,"http")));

        objMaper = new ObjectMapper();

    }

    @Test
    public void createCacheContentIndex() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest("wu-cache-content");

        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    builder.startObject("acctId");
                    {
                        builder.field("type", "long");
                    }
                    builder.endObject();
                    builder.startObject("apMac");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("cName");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("hits");
                    {
                        builder.field("type", "integer");
                    }
                    builder.endObject();
                    builder.startObject("misses");
                    {
                        builder.field("type", "integer");
                    }
                    builder.endObject();
                    builder.startObject("size");
                    {
                        builder.field("type", "long");
                    }
                    builder.endObject();
                    builder.startObject("ts");
                    {
                        builder.field("type", "date");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        request.mapping("_doc", builder);

        CreateIndexResponse response = client.indices().create(request);

        assertTrue(response.isAcknowledged());

    }


    @Test
    public void createContentAggrIndex() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest("wu-content-aggr");

        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    builder.startObject("acctId");
                    {
                        builder.field("type", "long");
                    }
                    builder.endObject();
                    builder.startObject("apMac");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("cName");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("hits");
                    {
                        builder.field("type", "integer");
                    }
                    builder.endObject();
                    builder.startObject("misses");
                    {
                        builder.field("type", "integer");
                    }
                    builder.endObject();
                    builder.startObject("size");
                    {
                        builder.field("type", "long");
                    }
                    builder.endObject();
                    builder.startObject("mTime");
                    {
                        builder.field("type", "date");
                    }
                    builder.endObject();
                    builder.startObject("cTime");
                    {
                        builder.field("type", "date");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        request.mapping("_doc", builder);

        CreateIndexResponse response = client.indices().create(request);

        assertTrue(response.isAcknowledged());


    }

    @Test
    public void existIndex() throws Exception {


        Response response = client.getLowLevelClient().performRequest("HEAD", "wu-cache-aggr");


        assertEquals(response.getStatusLine().getStatusCode(), RestStatus.OK.getStatus());


        SearchRequest searchRequest = new SearchRequest("temp");

        //UpdateRequest request = new UpdateRequest("tmep", "_doc", item.getId());

    }


    @Test
    public void updateByQuery() throws Exception {


//        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client.getLowLevelClient().);
//        updateByQuery.source("source_index")
//                .filter(QueryBuilders.termQuery("level", "awesome"))
//                .size(1000)
//                .script(new Script(ScriptType.INLINE, "ctx._source.awesome = 'absolutely'", "painless", Collections.emptyMap()));
        //BulkByScrollResponse response = updateByQuery.get();

        RestClient restClient = client.getLowLevelClient();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("query");
            {
                builder.startObject("bool");
                {
                    builder.startObject("filter");
                    {
                        builder.startObject("bool");
                        {
                            builder.startArray("must");
                            {
                                builder.startObject();
                                {
                                    builder.startObject("term").field("acctId", 4).endObject();
                                }
                                builder.endObject();
                                builder.startObject();
                                {
                                    builder.startObject("term").field("accumType", 1).endObject();
                                }
                                builder.endObject();
                                builder.startObject();
                                {
                                    builder.startObject("term").field("deleted",  Boolean.FALSE).endObject();
                                }
                                builder.endObject();
                                builder.startObject();
                                {
                                    builder.startObject("term").field("apMac", "B4:82:C5:00:24:71").endObject();
                                }
                                builder.endObject();
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();

            builder.startObject("script");
            {
                builder.field("source", "ctx._source.deleted = true");
                builder.field("lang", "painless");
            }
            builder.endObject();
        }
        builder.endObject();
        String jsonString = builder.string();
        LOG.info("jsonString is :" + jsonString);
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest("POST", "/wu-cache-accum/_doc/_update_by_query?conflicts=proceed", Collections.emptyMap(), entity);

        assertEquals(200, response.getStatusLine().getStatusCode());
    }


    @Test
    public void indexContentAggDocument() throws Exception {


        String time = "2018-06-21 02:00:00";
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dt = sf.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dt);
        Random random = new Random();

        ContentCacheData contentCache = new ContentCacheData();
        contentCache.setAcctId(1);
        contentCache.setcName("web-cache-file-0537-" + random.nextInt(1000));
        contentCache.setApMac("B4:82:C5:00:05:37");
        contentCache.setHits(10);
        contentCache.setMisses(15);
        contentCache.setSize(random.nextLong());
        contentCache.setmTime(calendar.getTimeInMillis());

        calendar.add(Calendar.MINUTE, 10);
        String jsonDoc = objMaper.writeValueAsString(contentCache);


        LOG.info("the json string is  {}", jsonDoc);

//        for(int i=0;  i< 50 ; i++) {
//            ContentCacheData contentCache = new ContentCacheData();
//            contentCache.setAcctId(1);
//            contentCache.setcName("web-cache-file-0537-"+random.nextInt(1000));
//            contentCache.setApMac("B4:82:C5:00:05:37");
//            contentCache.setHits(10+i);
//            contentCache.setMisses(15+i);
//            contentCache.setSize(random.nextLong());
//            contentCache.setmTime(calendar.getTimeInMillis());
//
//            calendar.add(Calendar.MINUTE, 10);
//            String jsonDoc = objMaper.writeValueAsString(contentCache);
//            IndexRequest request = new IndexRequest("content-aggr", "_doc");
//            request.source(jsonDoc, XContentType.JSON);
//
//            IndexResponse indexResponse =  client.index(request);
//            assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
//            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
//
//            LOG.info("the number of shared is {}, the successfully shared is {}" , shardInfo.getTotal(), shardInfo.getSuccessful());
//            assertEquals(shardInfo.getTotal(), shardInfo.getSuccessful());
//        }
    }


    @Test
    public void indexDocument() throws Exception {
        String time = "2018-06-20 14:00:00";
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dt = sf.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dt);

//        for(int i=0;  i< 20 ; i++) {
//            ApCacheData apCache = new ApCacheData();
//            apCache.setAcctId(1);
//            apCache.setApName("R2AP-B482C5-000537");
//            apCache.setApMac("B4:82:C5:00:05:37");
//            apCache.setHits(10+i);
//            apCache.setMisses(15+i);
//            apCache.setTs(calendar.getTimeInMillis());
//            apCache.setStrTime(sf.format(calendar.getTime()));
//            calendar.add(Calendar.MINUTE, 10);
//            String jsonDoc = objMaper.writeValueAsString(apCache);
//            IndexRequest request = new IndexRequest("content-cache", "10min");
//            request.source(jsonDoc, XContentType.JSON);
//
//            IndexResponse indexResponse =  client.index(request);
//            assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
//            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
//
//            LOG.info("the number of shared is {}, the successfully shared is {}" , shardInfo.getTotal(), shardInfo.getSuccessful());
//            assertEquals(shardInfo.getTotal(), shardInfo.getSuccessful());
//        }
    }


    @Test
    public void filterQuery() throws IOException {

        SearchRequest searchRequest = new SearchRequest("content-cache");
        searchRequest.types("10min");


        QueryBuilder qB = boolQuery().filter(
                boolQuery().must(termQuery("apName.keyword", "R2AP-B482C5-000537")));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(qB);
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        RestStatus status = searchResponse.status();

        LOG.info("the response status is {}", status);
        assertEquals(RestStatus.OK, status);

        SearchHits hits = searchResponse.getHits();

        LOG.info("the response search hit  is {}", hits);
        assertEquals(5, hits.getHits().length);

        assertEquals(0.0, hits.getMaxScore(), 0.0001F);
    }

    @Test
    public void queryUpdate() throws IOException {
        SearchRequest searchRequest = new SearchRequest("wu-cache-aggr");
        searchRequest.types("_doc");

        QueryBuilder qB = boolQuery().filter(
                boolQuery().must(termQuery("apMac", "B4:82:C5:00:24:78"))
                        .must(termQuery("acctId", 27263118L))
                        .must(termQuery("cName", "indeccode.png"))
                        .must(termQuery("ts", 1505275200000L)));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.version(true)
                .query(qB);

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        RestStatus status = searchResponse.status();

        LOG.info("the response status is {}", status);

        SearchHits hits = searchResponse.getHits();

        LOG.info("the response search hit  is {}", hits);

        assertEquals(1, hits.getHits().length);

//        SearchHit hit = hits.getAt(0);
//
//
//        LOG.info("the  hit  is {}" , hit);
//
//        //assertEquals(1, hit.getVersion());
//
//        assertEquals("SLivO2QBFKWYjm8CjlSY", hit.getId());
//
//        UpdateRequest request = new UpdateRequest("wu-cache-aggr", "_doc", hit.getId());
//                //.version(1);
//        Map<String, Object> parameters = new HashMap<>();
//
//        parameters.put("hits", 4);
//        parameters.put("misses", 5);
//        Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.hits += params.hits;ctx._source.misses += params.misses", parameters);
//
//        Map<String, Object> parameters1 = singletonMap("misses", 3);
//        //Script inline1 = new Script(ScriptType.INLINE, "painless", "ctx._source.misses += params.misses", parameters);
//        request.script(inline);//.script(inline1);
//
//        UpdateResponse updateResponse = client.update(request);
//        assertEquals(updateResponse.getResult(), DocWriteResponse.Result.UPDATED);


    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }
}
