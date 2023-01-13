package com.atguigu.es.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class EsTest_Doc_Update {
    public static void main(String[] args) throws Exception {
        // 创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 修改文档
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");
        // 这里doc可以理解为一条数据
        request.doc(XContentType.JSON,"sex","女");


        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);
        // 响应信息
        System.out.println(response.getResult());

        // 关闭客户端
        esClient.close();
    }
}
