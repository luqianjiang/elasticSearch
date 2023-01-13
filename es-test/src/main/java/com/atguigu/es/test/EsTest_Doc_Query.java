package com.atguigu.es.test;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class EsTest_Doc_Query {
    public static void main(String[] args) throws Exception {
        // 创建ES客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );

//        // 查询索引中全部数据
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        // 查询条件
//        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println("======================================");

//        // 条件查询:termQuery
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        // 查询条件
//        request.source(new SearchSourceBuilder()
//                .query(QueryBuilders.termQuery("age","30")));
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println("======================================");

//        // 分页查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        // 起始位置和每页数据：from公式：（当前页码-1）*size
//        builder.from(2);
//        builder.size(2);
//        request.source(builder);
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println("======================================");

//        // 查询排序
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        builder.from(0);
//        builder.size(4);
//        builder.sort("age", SortOrder.DESC);
//        request.source(builder);
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }


        System.out.println("======================================");
//
//        // 过滤字段
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
//        builder.from(0);
//        builder.size(4);
//        String[] include={"name"};
//        String[] exclude={};
//        builder.fetchSource(include,exclude);
//        request.source(builder);
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }


        System.out.println("======================================");

//        // 组合查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.must(QueryBuilders.matchQuery("age", 30));
//        boolQueryBuilder.must(QueryBuilders.matchQuery("sex", "男"));
//        builder.query(boolQueryBuilder);
//        request.source(builder);
//
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println("======================================");
//        // 范围查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
//        rangeQuery.gte(20);
//        rangeQuery.lte(30);
//        builder.query(rangeQuery);
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }


//        System.out.println("======================================");
//        // 模糊查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//        // 差一个可以匹配成功 fuzziness(Fuzziness.ONE)
//        FuzzyQueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery("name", "wangwu").fuzziness(Fuzziness.ONE);
//        builder.query(fuzzyQuery);
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println("======================================");
//        // 高亮查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//        TermsQueryBuilder termsQueryBuilder =
//                QueryBuilders.termsQuery("name","zhangsan");
//
//        //构建高亮字段
//        HighlightBuilder highlightBuilder = new HighlightBuilder();
//        highlightBuilder.preTags("<font color='red'>");//设置标签前缀
//        highlightBuilder.postTags("</font>");//设置标签后缀
//        highlightBuilder.field("name");//设置高亮字段
//        //设置高亮构建对象
//        builder.highlighter(highlightBuilder);
//        //设置查询方式
//        builder.query(termsQueryBuilder);
//
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }


        System.out.println("======================================");
//        // 聚合查询
//        SearchRequest request = new SearchRequest();
//        request.indices("user");
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//        // maxAge是别名，age是哪个字段求最大值
//        builder.aggregation(AggregationBuilders.max("maxAge").field("age"));
//
//
//        request.source(builder);
//        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
//
//        // 匹配的数据
//        SearchHits hits = response.getHits();
//        System.out.println(hits.getTotalHits());
//        for (SearchHit hit: hits) {
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println("======================================++++");
        // 分组查询
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // ageGroup是分组别名，age是哪个字段分组
        builder.aggregation(AggregationBuilders.terms("ageGroup").field("age"));


        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        // 匹配的数据
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        for (SearchHit hit: hits) {
            System.out.println(hit.getSourceAsString());
        }

        // 关闭客户端
        esClient.close();
    }
}
