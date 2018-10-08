package com.examples.transport.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TransportClientTest {


    private TransportClient client;

    @Before
    public void setUp() throws Exception {

        //byte[] bs = new byte[] { (byte) 192, (byte) 168, 20, 61 };

        Settings settings = Settings.builder().put("cluster.name","elastic-dev").build();

        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.20.61"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("10.22.0.52"), 9300));
    }


    @Test
    public void listNodes(){
        ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get();
        int numberOfNodes = healths.getNumberOfNodes();
        System.out.println("the number of node is "+ numberOfNodes);
        int numberOfDataNodes = healths.getNumberOfDataNodes();
        System.out.println("the number of data node is "+ numberOfDataNodes);
        List<DiscoveryNode> list =  client.listedNodes();
        for(DiscoveryNode node : list){
            System.out.println("nod ip is "+node.getHostAddress() +" the  node is master :"+node.isMasterNode() +"  the node is data is "+ node.isDataNode());
        }

    }


    @Test
    public void createIndex() throws Exception {

        CreateIndexRequest request = new CreateIndexRequest("content-cache");

        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("10min");
            {
                builder.startObject("properties");
                {
                    builder.startObject("acctId");
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



        request.mapping("10min", builder);

        IndicesAdminClient indicesAdminClient = client.admin().indices();

        CreateIndexResponse response = indicesAdminClient.create(request).get();

        assertTrue(response.isAcknowledged());


    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }
}
