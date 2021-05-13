package org.acme.service;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.apache.http.HttpHost;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Arrays;

@QuarkusMain
public class Main {

    public static void main(String ... args) {
        // register custom index template configuration for indexing large ngrams
        PutIndexTemplateRequest ngramTemplate = new PutIndexTemplateRequest("ngram-template")
                .patterns(Arrays.asList("oneaddress-*", "address-*"))
                .settings(Settings.builder().put("index.max_ngram_diff", 50));
        try {
            RestHighLevelClient restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().putTemplate(ngramTemplate, RequestOptions.DEFAULT);
            assert acknowledgedResponse.isAcknowledged();
        } catch (IOException theE) {
            theE.printStackTrace();
            throw new RuntimeException("Couldn't connect to the elasticsearch server to create necessary templates. Ensure the Elasticsearch user has permissions to create templates.");
        }
        Quarkus.run(args);
    }
}
