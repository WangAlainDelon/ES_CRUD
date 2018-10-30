package com.wx.es1;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class CRUDEs {
    private TransportClient client=null;
    @Before
    public void init()
    {
      try {
          Settings settings=Settings.builder()
                  .put("cluster.name","my-es")
                  .build();
          client=new PreBuiltTransportClient(settings).addTransportAddresses(
                  new InetSocketTransportAddress(InetAddress.getByName("192.168.203.128"),9300),
                  new InetSocketTransportAddress(InetAddress.getByName("192.168.203.129"),9300),
                  new InetSocketTransportAddress(InetAddress.getByName("192.168.203.130"),9300)
          );
      }catch (Exception e)
      {
          e.printStackTrace();
      }
    }
     /*
       关闭客户端
     */
    @After
    public void closeClient()
    {
        if (client!=null)
        {
            client.close();
        }
    }
    //插入一条数据
    @Test
    public void createIndex () throws Exception
    {
        IndexResponse response = client.prepareIndex("gamelog", "users", "2")
                .setSource(
                     jsonBuilder()
                                .startObject()
                                .field("username", "李四")
                                .field("gender", "male")
                                .field("birthday", new Date())
                                .field("fv", 9999)
                                .field("message", "trying out Elasticsearch")
                                .endObject()
                ).get();
    }
    /*
    查询一条数据
     */
    @Test
    public void getOne()
    {
        GetResponse response = client.prepareGet("gamelog", "users", "1").get();
        System.out.print(response);
    }
    /*
    查找多条数据
    */
    @Test
    public void getMulti() throws IOException
    {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("gamelog", "users", "1")
                .add("news", "fulltext", "2","3")
                .get();
        for(MultiGetItemResponse  responses: multiGetItemResponses) {
            GetResponse response = responses.getResponse();
            System.out.print(response);

        }
    }
    /*
    修改
     */
    @Test
    public void testUpdate() throws Exception {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("gamelog");
        updateRequest.type("users");
        updateRequest.id("1");
        updateRequest.doc(
                jsonBuilder()
                        .startObject()
                        .field("fv", 999.9)
                        .endObject());
        client.update(updateRequest).get();
    }
    /*
    删除
     */
    @Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete("gamelog", "users", "2").get();
        System.out.println(response);
    }
    /*
    指定条件删除
     */
    @Test
    public void testDeleteByQuery() {
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        //指定查询条件
                        .filter(QueryBuilders.matchQuery("username", "李四"))
                        //指定索引名称
                        .source("gamelog")
                        .get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }
    /*
    异步删除
     */
    //异步删除
    @Test
    public void testDeleteByQueryAsync() {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("gender", "male"))
                .source("gamelog")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        System.out.println("数据删除了");
                        System.out.println(deleted);
                    }
                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                    }
                });
        try {
            System.out.println("异步删除");
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /*
    范围内查询
     */
    @Test
    public void testRange() {

        QueryBuilder qb = rangeQuery("fv")
                // [88.99, 10000)
                .from(88.99)
                .to(10000)
                .includeLower(true)
                .includeUpper(false);

        SearchResponse response = client.prepareSearch("gamelog").setQuery(qb).get();

        System.out.println(response);
    }
    /*
        进行聚合查询 select team, count(*) as player_count from player group by team;
     */
    @Test
    public void testAgg1() {

        //指定索引和type
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //按team分组然后聚合，但是并没有指定聚合函数
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("player_count").field("team");
        //添加聚合器
        builder.addAggregation(teamAgg);
        //触发
        SearchResponse response = builder.execute().actionGet();
        //System.out.println(response);
        //将返回的结果放入到一个map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
//        Set<String> keys = aggMap.keySet();
//
//        for (String key: keys) {
//            System.out.println(key);
//        }

//        //取出聚合属性
        StringTerms terms = (StringTerms) aggMap.get("player_count");


        //
////        //依次迭代出分组聚合数据
//        for (Terms.Bucket bucket : terms.getBuckets()) {
//            //分组的名字
//            String team = (String) bucket.getKey();
//            //count，分组后一个组有多少数据
//            long count = bucket.getDocCount();
//            System.out.println(team + " " + count);
//        }

        Iterator<Terms.Bucket> teamBucketIt = terms.getBuckets().iterator();
        while (teamBucketIt .hasNext()) {
            Terms.Bucket bucket = teamBucketIt.next();
            String team = (String) bucket.getKey();

            long count = bucket.getDocCount();

            System.out.println(team + " " + count);
        }

    }

    //select team, position, count(*) as pos_count from player group by team, position;
    /*
      查找相同球队相同位置球员的数量
    */
    @Test
    public void testAgg2() {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //指定别名和分组的字段
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");
        TermsAggregationBuilder posAgg= AggregationBuilders.terms("pos_count").field("position");
        //添加两个聚合构建器
        builder.addAggregation(teamAgg.subAggregation(posAgg));
        //执行查询
        SearchResponse response = builder.execute().actionGet();
        //将查询结果放入map中
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //根据属性名到map中查找
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        //循环查找结果
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //先按球队进行分组
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            StringTerms positions = (StringTerms) subAggMap.get("pos_count");
            //因为一个球队有很多位置，那么还要依次拿出位置信息
            for (Terms.Bucket posBucket : positions.getBuckets()) {
                //拿到位置的名字
                String pos = (String) posBucket.getKey();
                //拿出该位置的数量
                long docCount = posBucket.getDocCount();
                //打印球队，位置，人数
                System.out.println(team + " " + pos + " " + docCount);
            }


        }
    }

}
