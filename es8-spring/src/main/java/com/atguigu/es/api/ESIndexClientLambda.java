package com.atguigu.es.api;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.*;
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

public class ESIndexClientLambda {
    private static ElasticsearchClient client;
    private static ElasticsearchAsyncClient asyncClient;
    private static ElasticsearchTransport transport;
    private static final String INDEX_ATGUIGU = "atguigu";

    public static void main(String[] args) throws Exception {
        initESConnection();
        operationIndexLambda();
    }

    private static void operationIndexLambda() throws Exception {

        final ElasticsearchIndicesClient indices = client.indices();

        // 判断索引是否存在
        final boolean flg = indices.exists(req -> req.index(INDEX_ATGUIGU)).value();
        if (flg) {
            System.out.println("索引已经被创建");
        } else {
            // 创建索引
            final CreateIndexResponse createIndexResponse = indices.create(req ->req.index(INDEX_ATGUIGU));
            System.out.println("创建索引成功：" + createIndexResponse.acknowledged());
        }

        // 查询索引
        final GetIndexResponse getIndexResponse = indices.get(req ->req.index(INDEX_ATGUIGU));
        System.out.println("索引查询成功：" + getIndexResponse.result());

        // 删除索引
        final DeleteIndexResponse delete = indices.delete(req ->req.index(INDEX_ATGUIGU));
        final boolean acknowledged = delete.acknowledged();
        System.out.println("删除索引成功：" + acknowledged);

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
