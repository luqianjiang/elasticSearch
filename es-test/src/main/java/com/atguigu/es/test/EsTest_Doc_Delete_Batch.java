package com.atguigu.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class EsTest_Doc_Delete_Batch {
    public static void main(String[] args) throws Exception {
        // 创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

        // 批量删除文档
        BulkRequest request = new BulkRequest();
        // 就是在BulkRequest批量添加单个indexRequest插入请求
        request.add(new DeleteRequest()
                .index("user")
                .id("1001")

        );
        request.add(new DeleteRequest()
                .index("user")
                .id("1002")

        );
        request.add(new DeleteRequest()
                .index("user")
                .id("1003")

        );

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);
        // 响应信息
        System.out.println(response.getTook());
        System.out.println(response.getItems());

        // 关闭客户端
        esClient.close();
    }
}
