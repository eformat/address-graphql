package org.acme.service;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.acme.entity.OneAddress;
import org.apache.http.util.EntityUtils;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.hibernate.search.mapper.orm.session.SearchSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Path("/index")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IndexResource {

    private final Logger log = LoggerFactory.getLogger(IndexResource.class);

    @Inject
    SearchSession searchSession;

    @Inject
    RestHighLevelClient restHighLevelClient;
    // FIXME need to figure out how to attach ssl, https://quarkus.io/guides/elasticsearch#programmatically-configuring-elasticsearch

    @Inject
    RestClient restClient;

    ActionListener<BulkByScrollResponse> reindexListener = new ActionListener<BulkByScrollResponse>() {
        @Override
        public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
            log.info(">>> clone reindex acknowledged " + bulkByScrollResponse.getStatus());
        }

        @Override
        public void onFailure(Exception e) {
            log.warn(">>> clone reindex failed" + e.getMessage());
        }
    };

    @GET
    @Path("/switch/{sourceIndex}/{targetIndex}")
    @Operation(operationId = "switch", summary = "switch index alias", description = "This operation switches the read and write alias", deprecated = false, hidden = false)
    public Response switchIndex(@PathParam("sourceIndex") String sourceIndex, @PathParam("targetIndex") String targetIndex) {
        if (sourceIndex.isBlank() || targetIndex.isBlank())
            return Response.notModified().build();
        log.info(">>> switching read and write alias for " + sourceIndex + " --> " + targetIndex);
        Pattern index = Pattern.compile("^(\\w+)-(\\d+)$");
        Matcher matchSource = index.matcher(sourceIndex);
        String sourceIndexName = new String();
        String sourceIndexNumber = new String();
        if (matchSource.find()) {
            sourceIndexName = matchSource.group(1);
            sourceIndexNumber = matchSource.group(2);
        } else {
            log.warn(">>> switch failed to match source index pattern (" + sourceIndex + ")");
            return Response.serverError().build();
        }
        Matcher matchTarget = index.matcher(sourceIndex);
        String targetIndexName = new String();
        String targetIndexNumber = new String();
        if (matchTarget.find()) {
            targetIndexName = matchSource.group(1);
            targetIndexNumber = matchSource.group(2);
        } else {
            log.warn(">>> switch failed to match target index pattern (" + targetIndex + ")");
            return Response.serverError().build();
        }
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions deleteReadAlias = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                .index(sourceIndex).alias(sourceIndexName + "-read");
        IndicesAliasesRequest.AliasActions deleteWriteAlias = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                .index(sourceIndex).alias(sourceIndexName + "-write");
        IndicesAliasesRequest.AliasActions addReadAlias = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .index(targetIndex).alias(targetIndexName + "-read");
        IndicesAliasesRequest.AliasActions addWriteAlias = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .index(targetIndex).alias(targetIndexName + "-write");
        indicesAliasesRequest.addAliasAction(deleteReadAlias);
        indicesAliasesRequest.addAliasAction(deleteWriteAlias);
        indicesAliasesRequest.addAliasAction(addReadAlias);
        indicesAliasesRequest.addAliasAction(addWriteAlias);
        try {
            AcknowledgedResponse indicesAliasesResponse =
                    restHighLevelClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.warn(">>> switch failed add/remove index alias (" + sourceIndex + ")->(" + targetIndex + ") " + e.getLocalizedMessage());
            return Response.serverError().build();
        }
        return Response.ok().build();
    }

    @GET
    @Path("/clone/{sourceIndex}/{targetIndex}")
    @Operation(operationId = "clone", summary = "clone an index", description = "This operation uses reindex to clone an elastic index asynchronously", deprecated = false, hidden = false)
    public Response cloneIndex(@PathParam("sourceIndex") String sourceIndex, @PathParam("targetIndex") String targetIndex) {
        if (sourceIndex.isBlank() || targetIndex.isBlank())
            return Response.notModified().build();
        log.info(">>> cloning " + sourceIndex + " --> " + targetIndex);
        // drop if it already exists
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(targetIndex);
        try {
            AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.info(">>> clone failed to delete target index, continuing (" + targetIndex + ") " + e.getLocalizedMessage());
        }
        // get source mapping
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(sourceIndex);
        GetMappingsResponse getMappingResponse;
        try {
            getMappingResponse = restHighLevelClient.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.warn(">>> clone failed to get source mapping (" + sourceIndex + ") " + e.getMessage());
            return Response.serverError().build();
        }
        // get source analysis settings
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(sourceIndex);
        GetSettingsResponse getSettingsResponse;
        try {
            getSettingsResponse = restHighLevelClient.indices().getSettings(getSettingsRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.warn(">>> clone failed to read source settings from index (" + targetIndex + ") " + e.getMessage());
            return Response.serverError().build();
        }
        // create new index
        CreateIndexRequest createRequest = new CreateIndexRequest(targetIndex);
        // put analysis onto target
        Settings indexSettings = getSettingsResponse.getIndexToSettings().get(sourceIndex);
        Settings analysisSettings = indexSettings.getAsSettings("index.analysis");
        Map<String, Object> allSettings = new HashMap<>();
        for (String key : analysisSettings.keySet()) {
            String value = analysisSettings.get(key);
            value = value.replaceAll("\\[", "").replaceAll("\\]", ""); // sob, but yes this is needed
            allSettings.put("index.analysis." + key, value);
        }
        // other setting we can parameterize/temlplatize
        //allSettings.put("index.number_of_shards", 1);
        //allSettings.put("index.number_of_replicas", 1);

        log.debug("***************All Settings*********************");
        for (String key : allSettings.keySet()) {
            log.debug(key + " : " + allSettings.get(key));
        }

        createRequest.settings(allSettings);
        // put mapping onto target
        Map<String, MappingMetadata> allMappings = getMappingResponse.mappings();
        MappingMetadata indexMapping = allMappings.get(sourceIndex);
        Map<String, Object> mapping = indexMapping.sourceAsMap();
        createRequest.mapping(mapping);
        // create index
        try {
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.warn(">>> clone failed to create target index (" + targetIndex + ") " + e.getMessage());
            return Response.serverError().build();
        }
        // reindex
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setDestIndex(targetIndex);
        reindexRequest.setScroll(TimeValue.timeValueMinutes(5));
        restHighLevelClient.reindexAsync(reindexRequest, RequestOptions.DEFAULT, reindexListener);
        return Response.ok().build();
    }


    @GET
    @Path("/massIndex/{numDocs}")
    @Operation(operationId = "massIndex", summary = "mass index documents", description = "This operation starts the mass indexer", deprecated = false, hidden = false)
    public Response massIndex(@PathParam("numDocs") @DefaultValue("20000") Integer numDocs) throws InterruptedException {
        searchSession.massIndexer()
                .batchSizeToLoadObjects(numDocs)
                .startAndWait();
        return Response.ok().build();
    }


    @GET
    @Path("/testLow/{search}")
    public List<OneAddress> testLow(@PathParam("search") String search) throws IOException {
        Request request = new Request(
                "POST",
                "/oneaddress-read/_search");
        //construct a JSON query {"query":{"match":{"address":{"query":"23 crank street"}}},"sort":["_score"],"_source":["*"],"suggest":{"address":{"prefix":"23 crank street","completion":{"field":"address_suggest"}}}}
        JsonObject termJson = new JsonObject().put("query", search.toLowerCase());
        JsonObject addressJson = new JsonObject().put("address", termJson);
        JsonObject matchJson = new JsonObject().put("match", addressJson);
        JsonObject fieldJson = new JsonObject().put("field", "address_suggest");
        JsonObject completionJson = new JsonObject().put("completion", fieldJson);
        JsonObject prefixJson = new JsonObject().put("prefix", search.toLowerCase()).mergeIn(completionJson);
        JsonObject suggestJson = new JsonObject().put("address", prefixJson);
        JsonObject queryJson = new JsonObject()
                .put("query", matchJson)
                .put("sort", new JsonArray().add("_score"))
                .put("_source", new JsonArray().add("*"))
                .put("suggest", suggestJson);
        request.setJsonEntity(queryJson.encode());
        org.elasticsearch.client.Response response = restClient.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonObject json = new JsonObject(responseBody);
        JsonArray hits = json.getJsonObject("hits").getJsonArray("hits");
        List<OneAddress> results = new ArrayList<>(hits.size());
        for (int i = 0; i < hits.size(); i++) {
            JsonObject hit = hits.getJsonObject(i);
            OneAddress address = hit.getJsonObject("_source").mapTo(OneAddress.class);
            results.add(address);
        }
        return results;
    }

    @GET
    @Path("/testHigh/{search}")
    public List<OneAddress> testHigh(@PathParam("search") String search) throws IOException {
        SearchRequest searchRequest = new SearchRequest("oneaddress-read");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //construct a JSON query {"match":{"address":{"query":"23 crank street"}}},"sort":["_score"],"_source":["*"],"suggest":{"address":{"prefix":"23 crank street","completion":{"field":"address_suggest"}}}
        JsonObject termJson = new JsonObject().put("query", search.toLowerCase());
        JsonObject addressJson = new JsonObject().put("address", termJson);
        JsonObject matchJson = new JsonObject().put("match", addressJson);
        JsonObject fieldJson = new JsonObject().put("field", "address_suggest");
        JsonObject completionJson = new JsonObject().put("completion", fieldJson);
        JsonObject prefixJson = new JsonObject().put("prefix", search.toLowerCase()).mergeIn(completionJson);
        JsonObject suggestAddressJson = new JsonObject().put("address", prefixJson);
        JsonObject suggestJson = new JsonObject().put("suggest", suggestAddressJson);
        JsonObject sortJson = new JsonObject().put("sort", new JsonArray().add("_score"));
        JsonObject sourceJson = new JsonObject().put("_source", new JsonArray().add("*"));
        String queryJson = String.join(",",
                matchJson.encode(),
                sortJson.encode(),
                sourceJson.encode(),
                suggestJson.encode());
        log.info(">>> JSON " + queryJson);
        searchSourceBuilder.query(QueryBuilders.wrapperQuery(queryJson));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        List<OneAddress> results = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits.getHits()) {
            String sourceAsString = hit.getSourceAsString();
            JsonObject json = new JsonObject(sourceAsString);
            results.add(json.mapTo(OneAddress.class));
        }
        return results;
    }

}
