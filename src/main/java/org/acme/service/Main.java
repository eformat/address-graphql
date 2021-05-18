package org.acme.service;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@QuarkusMain
public class Main {

    public static void main(String... args) {
        // register custom index template configuration for indexing large ngrams
        PutIndexTemplateRequest ngramTemplate = new PutIndexTemplateRequest("ngram-template")
                .patterns(Arrays.asList("oneaddress-*", "address-*"))
                .settings(Settings.builder().put("index.max_ngram_diff", 50));
        Pattern hostport = Pattern.compile("^(.*):(\\d+)$");
        String elasticCluster = System.getProperty("quarkus.elasticsearch.hosts");
        if (elasticCluster == null || elasticCluster.isEmpty()) {
            elasticCluster = "localhost:9200";
        }
        Matcher matchAddress = hostport.matcher(elasticCluster);
        String host = new String();
        Integer port = 0;
        if (matchAddress.find()) {
            host = matchAddress.group(1);
            port = Integer.parseInt(matchAddress.group(2));
        }
        String elasticClusterUsername = System.getProperty("quarkus.elasticsearch.username");
        String elasticClusterPassword = System.getProperty("quarkus.elasticsearch.password");
        String elasticScheme = System.getProperty("quarkus.elasticsearch.protocol");
        RestClientBuilder builder;
        if (elasticScheme != null && !elasticScheme.isEmpty()) {
            builder = RestClient.builder(new HttpHost(host, port, elasticScheme));
        } else {
            builder = RestClient.builder(new HttpHost(host, port, "http"));
        }
        // ignore credentials and https if not set
        if (elasticClusterPassword != null && !elasticClusterPassword.isEmpty()
                && elasticClusterUsername != null && !elasticClusterUsername.isEmpty()
                && elasticScheme != null && !elasticScheme.isEmpty()) {

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(elasticClusterUsername, elasticClusterPassword));

            SSLContext sslContext;
            try {
                sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[]{ TrustAllX509ExtendedTrustManager.getInstance() }, null);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                throw new RuntimeException();
            } catch (KeyManagementException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
            builder.
                    setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(
                                HttpAsyncClientBuilder httpClientBuilder) {
                            return httpClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider)
                                    .setSSLContext(sslContext)
                                    .setSSLHostnameVerifier((host, session) -> true);
                        }
                    });
        }
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);
        try {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().putTemplate(ngramTemplate, RequestOptions.DEFAULT);
            assert acknowledgedResponse.isAcknowledged();
        } catch (IOException theE) {
            theE.printStackTrace();
            throw new RuntimeException("Couldn't connect to the elasticsearch server to create necessary templates. Ensure the Elasticsearch user has permissions to create templates.");
        }
        Quarkus.run(MyApp.class, args);
    }

    public static class MyApp implements QuarkusApplication {
        @Override
        public int run(String... args) {
            Quarkus.waitForExit();
            return 0;
        }
    }
}
