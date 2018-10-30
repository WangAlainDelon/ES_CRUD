package com.wx.es1;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class AdminAPI {
    /*
    AdminApi主要就是用来操作创建索引分片之类的admin操作
     */
   //初始化集群设置
    private TransportClient client;
    @Before
    public void init()throws Exception{
        Settings settings=Settings.builder().put("cluster.name","my-es").build();
        client=new PreBuiltTransportClient(settings).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName("192.168.203.128"),9300),
                new InetSocketTransportAddress(InetAddress.getByName("192.168.203.129"),9300),
                new InetSocketTransportAddress(InetAddress.getByName("192.168.203.130"),9300)
        );
    }

    //创建索引，并配置一些参数
    @Test
    public void createIndexWithSettings() {
        //获取Admin的API
        AdminClient admin = client.admin();
        //使用Admin API对索引进行操作
        IndicesAdminClient indices = admin.indices();
        //准备创建索引
        indices.prepareCreate("gamelog")
                //配置索引参数
                .setSettings(
                        //参数配置器
                        Settings.builder()//指定索引分区的数量
                                .put("index.number_of_shards", 4)
                                //指定索引副本的数量（注意：不包括本身，如果设置数据存储副本为2，实际上数据存储了3份）
                                .put("index.number_of_replicas", 2)
                )
                //真正执行
                .get();
    }
    /**
     * 你可以通过dynamic设置来控制这一行为，它能够接受以下的选项：
     * true：默认值。动态添加字段
     * false：忽略新字段
     * strict：如果碰到陌生字段，抛出异常
     * @throws
     */
    //创建索引和mapping映射
    @Test
    public void createSettingsMappings() throws Exception {
        //1.设置Settings
        HashMap<String, Object> settings_map = new HashMap<String, Object>();
        //设置分片的数量
        settings_map.put("number_of_shards", 3);
        //设置副本的数量，如果设置两个副本就代表会存三份数据
        settings_map.put("number_of_replicas", 2);
        //2.这是mapping映射,这相当于配置一个域信息
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("dynamic", true)
                //设置type中的属性
                .startObject("properties")
                //设置num属性的信息
                .startObject("num")
                //类型是Integer
                .field("type", "integer")
                //不分词，但是建索引
                .field("index", "not_analyzed")
                //在文档中存储
                .field("store", "yes")
                .endObject()
                //设置name属性的信息
                .startObject("name")
                //类型为string
                .field("type", "string")
                //在文档中存储
                .field("store", "yes")
                //建立索引，分词
                .field("index", "analyzed")
                //分词器使用ik
                .field("analyzer", "ik_max_word")
                .endObject()
                .endObject()
                .endObject();
        //创建索引,名字叫user_info
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate("user_info");
        //管理索引（user_info）然后关联type（user）
        builder.setSettings(settings_map).addMapping( "user",contentBuilder).get();
    }

    /**
     * index这个属性，no代表不建索引
     * not_analyzed，建索引不分词
     * analyzed 即分词，又建立索引
     * expected [no], [not_analyzed] or [analyzed]
     * @throws IOException
     */

    @Test
    public void testSettingsPlayerMappings() throws IOException {
        //1:settings
        HashMap<String, Object> settings_map = new HashMap<String, Object>(2);
        settings_map.put("number_of_shards", 3);
        settings_map.put("number_of_replicas", 1);

        //2:mappings
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()//
                .field("dynamic", "true")
                .startObject("properties")
                .startObject("id")
                .field("type", "integer")
                .field("store", "yes")
                .endObject()
                .startObject("name")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .startObject("salary")
                .field("type", "integer")
                .endObject()
                .startObject("team")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("position")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("description")
                .field("type", "string")
                .field("store", "no")
                .field("index", "analyzed")
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("addr")
                .field("type", "string")
                .field("store", "yes")
                .field("index", "analyzed")
                .field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject();

        CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate("player_info");
        prepareCreate.setSettings(settings_map).addMapping("player", builder).get();

    }
    //select team, max(age) as max_age from player group by team;查询每个队年龄最大的球员
    @Test
    public void testAgg3() {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //指定安球队进行分组
        TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team_name").field("team");
        //指定分组求最大值
        MaxAggregationBuilder maxAgg = AggregationBuilders.max("max_age").field("age");
        //分组后求最大值
        builder.addAggregation(teamAgg.subAggregation(maxAgg));
        //查询
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //根据team属性，获取map中的内容
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //分组的属性名
            String team = (String) teamBucket.getKey();
            //在将聚合后取最大值的内容取出来放到map中
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            //取分组后的最大值
            InternalMax ages = (InternalMax)subAggMap.get("max_age");
            double max = ages.getValue();
            System.out.println(team + " " + max);
        }
    }
    //select team, avg(age) as avg_age, sum(salary) as total_salary from player group by team;
    //查询每个队的平均年龄和平均工资
    @Test
    public void testAgg4() {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //指定分组字段
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team");
        //指定聚合函数是求平均数据
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avg_age").field("age");
        //指定另外一个聚合函数是求和
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
        //分组的聚合器关联了两个聚合函数
        builder.addAggregation(termsAgg.subAggregation(avgAgg).subAggregation(sumAgg));
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //按分组的名字取出数据
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //获取球队名字
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            //根据别名取出平均年龄
            InternalAvg avgAge = (InternalAvg)subAggMap.get("avg_age");
            //根据别名取出薪水总和
            InternalSum totalSalary = (InternalSum)subAggMap.get("total_salary");
            double avgAgeValue = avgAge.getValue();
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + " " + avgAgeValue + " " + totalSalaryValue);
        }
    }

    //select team, sum(salary) as total_salary from player group by team order by total_salary desc;
    //查询每个队的队员的总工资并按升序排列
    @Test
    public void testAgg5() {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //按team进行分组，然后指定排序规则
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team").order(Terms.Order.aggregation("total_salary ", true));
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
        builder.addAggregation(termsAgg.subAggregation(sumAgg));
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            InternalSum totalSalary = (InternalSum)subAggMap.get("total_salary");
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + " " + totalSalaryValue);
        }
    }
}
