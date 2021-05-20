package org.acme.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.acme.entity.Address;
import org.acme.entity.OneAddress;
import org.acme.entity.StreetType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.hibernate.search.backend.elasticsearch.ElasticsearchExtension;
import org.hibernate.search.backend.elasticsearch.search.query.ElasticsearchSearchResult;
import org.hibernate.search.engine.search.common.BooleanOperator;
import org.hibernate.search.mapper.orm.session.SearchSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@GraphQLApi
public class AddressResource {

    private final Logger log = LoggerFactory.getLogger(AddressResource.class);

    @Inject
    SearchSession searchSession;

    @Inject
    RestHighLevelClient restHighLevelClient;

    @ConfigProperty(name = "quarkus.hibernate-search-orm.schema-management.strategy")
    String indexLoadStrategy;

    @ConfigProperty(name = "index.onstart")
    String indexOnStart;

    @Transactional
    void onStart(@Observes StartupEvent ev) throws InterruptedException {
        // only reindex if we imported some content and we set index.onstart to true
        if (Address.count() > 0 && !indexLoadStrategy.equals("none") && indexOnStart.equalsIgnoreCase("true")) {
            searchSession.massIndexer()
                    .batchSizeToLoadObjects(20000)
                    .startAndWait();
        }
    }

    /*
      BEWARE - This is useful code and works OK, but under concurrent load performs a lot worse than directly using High Level Rest Client
     */
    private ElasticsearchSearchResult<JsonObject> _search(String finalSearch, Optional<Integer> size) {
        return Uni.createFrom().item(
                searchSession.search(OneAddress.class)
                        .extension(ElasticsearchExtension.get())
                        .select(f -> f.source())
                        .where((finalSearch == null || finalSearch.isEmpty()) ?
                                f -> f.matchAll()
                                : f -> f.match()
                                .field("address")
                                .matching(finalSearch)
                        )
                        .requestTransformer(context -> {
                            JsonObject body = context.body();
                            body.add("suggest", jsonObject(suggest -> {
                                suggest.add("address", jsonObject(mySuggest -> {
                                    mySuggest.addProperty("prefix", finalSearch);
                                    mySuggest.add("completion", jsonObject(term -> {
                                        term.addProperty("field", "address_suggest");
                                    }));
                                }));
                            }));
                        })
                        .sort(f -> f.score())
                        .fetch(size.orElse(20))
        ).runSubscriptionOn(Infrastructure.getDefaultWorkerPool()).await().indefinitely();
    }

    private List<OneAddress> _processSearch(ElasticsearchSearchResult<JsonObject> result) {
        JsonArray matches = result.responseBody()
                .getAsJsonObject("hits")
                .getAsJsonArray("hits");

        JsonArray suggestions = result.responseBody()
                .getAsJsonObject("suggest")
                .getAsJsonArray("address").get(0).getAsJsonObject()
                .getAsJsonArray("options");

        HashSet<OneAddress> uniqueList = new HashSet<>();
        for (JsonElement element : matches) {
            OneAddress address = new OneAddress();
            address.setAddress(element.getAsJsonObject().get("_source").getAsJsonObject().get("address").getAsString());
            address.setScore(element.getAsJsonObject().get("_score").getAsBigDecimal());
            uniqueList.add(address);
        }
        for (JsonElement element : suggestions) {
            OneAddress address = new OneAddress();
            address.setAddress(element.getAsJsonObject().get("text").getAsString());
            address.setScore(element.getAsJsonObject().get("_score").getAsBigDecimal());
            uniqueList.add(address);
        }
        List<OneAddress> list = new ArrayList<>(uniqueList);
        Collections.sort(list);
        log.info(">>> returning list " + list.size());
        return list;
    }

    private List<OneAddress> _fetchHighLevelClient(String search) {
        SearchRequest searchRequest = new SearchRequest("oneaddress-read");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String queryJson;
        if (search == null || search.isEmpty()) {
            io.vertx.core.json.JsonObject matchAll = new io.vertx.core.json.JsonObject().put("match_all", new io.vertx.core.json.JsonObject());
            queryJson = matchAll.encode();
        } else {
            //construct a JSON query {"match":{"address":{"query":"23 crank street"}}},"sort":["_score"],"_source":["*"],"suggest":{"address":{"prefix":"23 crank street","completion":{"field":"address_suggest"}}}
            io.vertx.core.json.JsonObject termJson = new io.vertx.core.json.JsonObject().put("query", search.toLowerCase());
            io.vertx.core.json.JsonObject addressJson = new io.vertx.core.json.JsonObject().put("address", termJson);
            io.vertx.core.json.JsonObject matchJson = new io.vertx.core.json.JsonObject().put("match", addressJson);
            io.vertx.core.json.JsonObject fieldJson = new io.vertx.core.json.JsonObject().put("field", "address_suggest");
            io.vertx.core.json.JsonObject completionJson = new io.vertx.core.json.JsonObject().put("completion", fieldJson);
            io.vertx.core.json.JsonObject prefixJson = new io.vertx.core.json.JsonObject().put("prefix", search.toLowerCase()).mergeIn(completionJson);
            io.vertx.core.json.JsonObject suggestAddressJson = new io.vertx.core.json.JsonObject().put("address", prefixJson);
            io.vertx.core.json.JsonObject suggestJson = new io.vertx.core.json.JsonObject().put("suggest", suggestAddressJson);
            io.vertx.core.json.JsonObject sortJson = new io.vertx.core.json.JsonObject().put("sort", new io.vertx.core.json.JsonArray().add("_score"));
            io.vertx.core.json.JsonObject sourceJson = new io.vertx.core.json.JsonObject().put("_source", new io.vertx.core.json.JsonArray().add("*"));
            queryJson = String.join(",",
                    matchJson.encode(),
                    sortJson.encode(),
                    sourceJson.encode(),
                    suggestJson.encode());
        }
        log.info(">>> JSON " + queryJson);
        searchSourceBuilder.query(QueryBuilders.wrapperQuery(queryJson));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
        SearchHits hits = searchResponse.getHits();
        List<OneAddress> results = new ArrayList<>(hits.getHits().length);
        for (SearchHit hit : hits.getHits()) {
            String sourceAsString = hit.getSourceAsString();
            float score = hit.getScore();
            io.vertx.core.json.JsonObject json = new io.vertx.core.json.JsonObject(sourceAsString);
            OneAddress address = json.mapTo(OneAddress.class);
            address.setScore(BigDecimal.valueOf(score));
            results.add(address);
        }
        // FIXME - resort based on _score, annoying since its sorted from ES
        results.sort(Comparator.reverseOrder());
        log.info(">>> search returned");
        return results;
    }

    /*
      Search using elastic completion suggester.
      We want ONE call to ES here to keep things fast.
      This requires the data is engineered into a single oneaddress.address text field first.
     */
    @Query
    @Description("Search oneaddress by search term")
    public List<OneAddress> oneaddresses(@Name("search") String search, @Name("size") Optional<Integer> size) {
        String finalSearch = (search == null) ? "" : search.trim().toLowerCase();
        log.info(">>> Final Search Words: finalSearch(" + finalSearch + ")");
        return _fetchHighLevelClient(search);

        // Does not perform as well
        //ElasticsearchSearchResult<JsonObject> result = _search(finalSearch, size);
        //log.info(">>> search returned");
        //return _processSearch(result);
    }

    /*
      Search by individual fields. Raw data provided in this format, so very little pre-processsing.
      We want ONE call to ES here to keep things fast.
      We do some pre-processing on the search term, and we assume the caller is hitting search on each character input.
      Wildcard match performed on numbers. Fuzzy() matching performed on each name type term, so can handle spelling mistakes.

          [flat number]/[street number] [street name] [street type name] [suburb name]

      Example addresses searches we can cater for:

      lower red hill road wondai
      45/15 breaker street main beach
      11 oyster cove promenade helensvale

      Doesn't handle:
      - AUS states (QLD,NSW etc) not handled yet
      - postcodes
     */
    @Query
    @Description("Search addresses by street number, name, type, suburb")
    public List<Address> addresses(@Name("search") String search, @Name("size") Optional<Integer> size) {
        // trim leading and trialing spaces
        search = search.trim();
        // break search string into - optional flat digits, optional '/' flat/unit separator, optional house number digit, rest of search words
        Pattern address = Pattern.compile("^(\\d+)?(\\/)?(\\d+)?\\s?(.*)$");
        Matcher matchAddress = address.matcher(search);
        String num = new String();
        String flat = new String();
        String rest = new String();
        if (matchAddress.find()) {
            num = matchAddress.group(1);
            if (matchAddress.group(2) != null && matchAddress.group(2).equals("/")) {
                if (matchAddress.group(3) == null) {
                    // special case where we search for "22/" i.e. the start of a flat/unit/shop
                    flat = matchAddress.group(1);
                    num = null;
                } else {
                    flat = matchAddress.group(1);
                    num = matchAddress.group(3);
                }
            }
            rest = matchAddress.group(4);
        }
        // split on StreetType so we can figure out the "street name" vs "suburb name" bits
        String[] parts = {"", ""};
        String street = StreetType.instance().matches(search);
        if (street != null && !street.trim().isEmpty()) {
            // deal with multi named streets e.g. "andrew campbell drive"
            parts = rest.split(street, -1);
        } else {
            parts[0] = rest;
        }
        // logic for searching with a streetType and no street name where streetType is actually part of the street name
        // e.g. "22 foobar break" vs "22 breaker street"
        // assume we search for "22 break" for example
        if (parts[0].isEmpty() && !street.isEmpty()) {
            parts[0] = street;
            street = null;
        }
        // get final strings
        String finalNum = (num != null ? num.trim().toLowerCase() : new String());
        String finalFlat = (flat != null ? flat.trim().toLowerCase() : new String());
        String finalLoc = (parts[0] != null ? parts[0].trim().toLowerCase() : new String());
        String finalStreet = (street != null ? street.trim().toLowerCase() : new String());
        String finalSuburb = (parts[1] != null ? parts[1].trim().toLowerCase() : new String());
        log.info(">>> Final Search Words: " + "finalNum(" + finalNum + ") finalFlat(" + (finalFlat.trim().isEmpty() ? "" : finalFlat) + ") finalLoc(" + finalLoc + ") finalStreet(" + finalStreet + ") finalSuburb(" + finalSuburb + ")");

        return searchSession.search(Address.class)
                .extension(ElasticsearchExtension.get())
                .where(f -> ((finalNum == null || finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc == null || finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search all if empty terms
                        f.matchAll()
                        : ((finalNum != null || !finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc == null || finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by street number only
                        f.simpleQueryString()
                                .field("number_first")
                                .matching(finalNum + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat != null || !finalFlat.isEmpty()) && (finalLoc == null || finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by street number only
                        f.simpleQueryString()
                                .field("flat_number")
                                .matching(finalFlat + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty())) ? // search by location only
                        f.simpleQueryString()
                                .field("street_name")
                                .matching(finalLoc + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum == null || finalNum.trim().isEmpty()) && (finalLoc == null || finalLoc.trim().isEmpty()) && (finalStreet != null || !finalStreet.trim().isEmpty()) && (finalSuburb == null || finalSuburb.trim().isEmpty())) ? // search by street type only
                        f.match()
                                .field("street_type_code")
                                .matching(finalStreet)
                                .fuzzy()
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc == null || finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb != null || !finalSuburb.isEmpty())) ? // search by suburb only
                        f.match()
                                .field("locality_name")
                                .matching(finalSuburb)
                                .fuzzy()
                        : ((finalNum != null || !finalNum.isEmpty()) && (finalFlat != null || !finalFlat.isEmpty()) && (finalLoc == null || finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by flat/unit number, street number only
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("flat_number")
                                        .matching(finalFlat + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("number_first")
                                        .matching(finalNum + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                        : ((finalNum != null || !finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by street number, street number only
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("number_first") //.boost(2.0f)
                                        .matching(finalNum + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat != null || !finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by flat/unit number, street number only
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("flat_number")
                                        .matching(finalFlat + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet != null || !finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by street name, street type
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet != null || !finalStreet.isEmpty()) && (finalSuburb != null || !finalSuburb.isEmpty())) ? // search by street name, street type, suburb
                        f.bool()
                                .must(f.match()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc)
                                        .fuzzy())
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                                .must(f.match()
                                        .field("locality_name")
                                        .matching(finalSuburb)
                                        .fuzzy())
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat != null || !finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet != null || !finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by street number, street name, street type
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("flat_number")
                                        .matching(finalFlat + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                        : ((finalNum != null || !finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet != null || !finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by street number, street name, street type
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("number_first")//.boost(2.0f)
                                        .matching(finalNum) // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                        : ((finalNum != null || !finalNum.isEmpty()) && (finalFlat != null || !finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet == null || finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by final number, street number, street name
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("flat_number")
                                        .matching(finalFlat + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f)
                                        .matching(finalNum + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                        : ((finalNum != null || !finalNum.isEmpty()) && (finalFlat != null || !finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet != null || !finalStreet.isEmpty()) && (finalSuburb == null || finalSuburb.isEmpty())) ? // search by flat number, street number, street name, street type
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("flat_number")
                                        .matching(finalFlat + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f)
                                        .matching(finalNum + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                        : ((finalNum != null || !finalNum.isEmpty()) && (finalFlat == null || finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet != null || !finalStreet.isEmpty()) && (finalSuburb != null || !finalSuburb.isEmpty())) ? // search by street number, street name, street type, suburb
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f)
                                        .matching(finalNum + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                                .must(f.match()
                                        .field("locality_name")
                                        .matching(finalSuburb)
                                        .fuzzy())
                        : ((finalNum == null || finalNum.isEmpty()) && (finalFlat != null || !finalFlat.isEmpty()) && (finalLoc != null || !finalLoc.isEmpty()) && (finalStreet != null || !finalStreet.isEmpty()) && (finalSuburb != null || !finalSuburb.isEmpty())) ? // search by flat number, street name, street type, suburb
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("flat_number")
                                        .matching(finalNum + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                                .must(f.match()
                                        .field("locality_name")
                                        .matching(finalSuburb)
                                        .fuzzy())
                        : // search by everything ~logical AND
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("flat_number")
                                        .matching(finalFlat + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f)
                                        .matching(finalNum + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND)) // default is OR, we want and for wildcard)
                                .must(f.simpleQueryString()
                                        .field("street_name").boost(2.0f) // boost location score
                                        .matching(finalLoc + "*")
                                        .defaultOperator(BooleanOperator.AND))
                                .must(f.match()
                                        .field("street_type_code")
                                        .matching(finalStreet)
                                        .fuzzy())
                                .must(f.match()
                                        .field("locality_name")
                                        .matching(finalSuburb)
                                        .fuzzy())
                )
                .sort(f -> (finalFlat.isEmpty() ? f.field("number_first_sort").then().field("street_name_sort")
                        : f.field("number_first_sort").then().field("flat_number_sort").then().field("street_name_sort")))
                .fetchHits(size.orElse(20));
    }

    private static JsonObject jsonObject(Consumer<JsonObject> instructions) {
        JsonObject object = new JsonObject();
        instructions.accept(object);
        return object;
    }
}
