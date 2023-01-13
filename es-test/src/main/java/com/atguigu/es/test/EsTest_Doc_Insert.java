package com.atguigu.es.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;

public class EsTest_Doc_Insert {
    public static void main(String[] args) throws Exception {
        // 创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 创建文档
        IndexRequest request = new IndexRequest();
        // 哪个索引，并且给数据的一个id
        request.index("user").id("1001");
        // 准备一条数据对象
        User user = new User();
        user.setName("zhangsan");
        user.setAge(30);
        user.setSex("男");
        // 向es插入数据，需要转换为json格式
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        // 放入请求体中,并且设置json类型
        request.source(userJson,XContentType.JSON);
        // 插入数据，使用index
        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
        // 响应信息
        System.out.println(response.getResult());

        // 关闭客户端
        esClient.close();
    }
}
