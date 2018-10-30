package com.wx.es1;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;

public class HelloEs {
    /*
      1.连接es集群，通过id查询数据
     */
    @Test
    public void helloES()
    {
        TransportClient client=null;
        try {
            //1.设置集群的配置信息
            Settings settings=Settings.builder()
                    .put("cluster.name","my-es")
                    .build();
            //2.连接集群,创建客户端
            client=new PreBuiltTransportClient(settings).addTransportAddresses(
                    new InetSocketTransportAddress(InetAddress.getByName("192.168.203.128"),9300),
                    new InetSocketTransportAddress(InetAddress.getByName("192.168.203.129"),9300),
                    new InetSocketTransportAddress(InetAddress.getByName("192.168.203.130"),9300));
            //3.使用客户端操作写操作语句，操作集群 .actionGet()方法是同步的，没有返回就等待
            GetResponse response = client.prepareGet("news", "fulltext", "2").execute().actionGet();
            //4.打印执行结果
            System.out.print(response);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally {
            client.close();
        }
    }
}
