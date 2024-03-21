package com.atguigu.es.api;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;

public class ESDocClient {
    private static ElasticsearchClient client;
    private static ElasticsearchAsyncClient asyncClient;
    private static ElasticsearchTransport transport;
    private static final String INDEX_ATGUIGU = "atguigu";

    public static void main(String[] args) throws Exception {
        initESConnection();
        //operationDox();
        queryDocument();
    }

    private static void queryDocument() throws Exception {
        MatchQuery matchQuery = new MatchQuery.Builder()
                .field("age")
                .query(FieldValue.of(31))
                .build();
        Query query = new Query.Builder().match(matchQuery).build();
        SearchRequest searchRequest = new SearchRequest.Builder().query(query).build(); // 可以不用指定索引，默认在全部索引上
        final SearchResponse<Object> search = client.search(searchRequest, Object.class);
        System.out.println(search);

        transport.close();
    }

    private static void operationDox() throws Exception {
//        // 创建文档
//        User user = new User(1001, "lisi", 10);
//        CreateRequest createRequest = new CreateRequest.Builder().index(INDEX_ATGUIGU).id(user.getId().toString()).document(user).build();
//        CreateResponse createResponse = client.create(createRequest);
//        System.out.println(createResponse);

//        // 批量创建文档
//        final List<BulkOperation> operations = new ArrayList<BulkOperation>();
//        for (int i = 1; i <= 5; i++) {
//            final CreateOperation.Builder builder = new CreateOperation.Builder();
//            builder.index(INDEX_ATGUIGU);
//            builder.id("200" + i);
//            builder.document(new User(2000 + i, "zhangsan" + i, 30 + i * 10));
//            final CreateOperation<Object> objectCreateOperation = builder.build();
//            final BulkOperation bulk = new BulkOperation.Builder().create(objectCreateOperation).build();
//            operations.add(bulk);
//        }
//        BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).build();
//        final BulkResponse bulkResponse = client.bulk(bulkRequest);
//        System.out.println("数据操作成功：" + bulkResponse);

//        // 删除文档
//        DeleteRequest deleteRequest = new DeleteRequest.Builder().index(INDEX_ATGUIGU).id("1001").build();
//        client.delete(deleteRequest);

        // 文档查询

        transport.close();
    }

    private static void initESConnection() throws Exception {
        //获取客户端对象
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "fqe3gFoXy7kVtOIyDUJN"));
        Path caCertificatePath = Paths.get("es8-spring/certs/es-api-ca.crt");
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("192.168.23.131", 9200, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setSSLContext(sslContext).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        RestClient restClient = builder.build();
        transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);
        //asyncClient = new ElasticsearchAsyncClient(transport);
        //transport.close();
    }
}
