package com.atguigu.es.api;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CreateResponse;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
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

public class ESDocLambdaClient {
    private static ElasticsearchClient client;
    private static ElasticsearchAsyncClient asyncClient;
    private static ElasticsearchTransport transport;
    private static final String INDEX_ATGUIGU = "atguigu";

    public static void main(String[] args) throws Exception {
        initESConnection();
        //operationDoxLambda();
        queryDocumentLambda();
    }

    private static void queryDocumentLambda() throws Exception {
        HitsMetadata<Object> res = client.search(
                req -> {
                    req.query(
                            q ->
                                    q.match(m -> m.field("age").query(31))
                    );
                    return req;
                }
                , Object.class
        ).hits();
        System.out.println(res);

        transport.close();
    }
    private static void operationDoxLambda() throws Exception {
//        // 创建文档
//        User user = new User(1001, "lisi", 10);
//        CreateResponse createResponse = client.create(
//                req -> req.index(INDEX_ATGUIGU)
//                        .id(user.getId().toString())
//                        .document(user));
//        System.out.println(createResponse);

//        // 批量创建文档
//        List<User> users = new ArrayList<User>();
//
//        for (int i = 1; i <= 5; i++) {
//            // 每个用户的id从3001开始递增，名字为lisi加上循环变量i的值，年龄为30加上循环变量i的值
//            users.add(new User(3000 + i, "wangwu" + i, 30 + i));
//        }
//        client.bulk(req -> {
//                    users.forEach(u -> {
//                                req.operations(b -> {
//                                            b.create(
//                                                    d -> d.id(u.getId().toString()).index(INDEX_ATGUIGU).document(u));
//                                            return b;
//                                        }
//                                );
//                            }
//                    );
//                    return req;
//                }
//        );

        // 删除文档
        client.delete(
                req -> req.index(INDEX_ATGUIGU).id("1001") );

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
