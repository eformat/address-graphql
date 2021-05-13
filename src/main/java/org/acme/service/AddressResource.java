package org.acme.service;

import com.arjuna.ats.internal.jdbc.drivers.modifiers.list;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.quarkus.runtime.StartupEvent;
import org.acme.entity.Address;
import org.acme.entity.OneAddress;
import org.acme.entity.StreetType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;
import org.hibernate.search.backend.elasticsearch.ElasticsearchExtension;
import org.hibernate.search.backend.elasticsearch.search.query.ElasticsearchSearchResult;
import org.hibernate.search.engine.search.common.BooleanOperator;
import org.hibernate.search.mapper.orm.session.SearchSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@GraphQLApi
public class AddressResource {

    private final Logger log = LoggerFactory.getLogger(AddressResource.class);

    @Inject
    SearchSession searchSession;

    @ConfigProperty(name = "quarkus.hibernate-search-orm.schema-management.strategy")
    String indexLoadStrategy;

    @Transactional
    void onStart(@Observes StartupEvent ev) throws InterruptedException {
        // only reindex if we imported some content
        if (Address.count() > 0 && !indexLoadStrategy.equals("none")) {
            searchSession.massIndexer()
                    .batchSizeToLoadObjects(100000)
                    .startAndWait();
        }
    }

    /*
      Search using elastic completion suggester.
      We want ONE call to ES here to keep things fast.
      This requires the data is engineered into a single oneaddress.address text field first.
     */
    @Query
    @Description("Search oneaddress by search term")
    public List<OneAddress> oneaddresses(@Name("search") String search, @Name("size") Optional<Integer> size) {
        String finalSearch = search.trim().toLowerCase();
        log.info(">>> Final Search Words: finalSearch(" + finalSearch + ")");

        ElasticsearchSearchResult<OneAddress> result = searchSession.search(OneAddress.class)
                .extension(ElasticsearchExtension.get())
                .where(f -> f.matchAll())
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
                .fetch(20);

        JsonArray suggestions = result.responseBody()
                .getAsJsonObject("suggest")
                .getAsJsonArray("address").get(0).getAsJsonObject()
                .getAsJsonArray("options");

        List<OneAddress> list = new ArrayList<>();
        for (JsonElement element : suggestions) {
            OneAddress address = new OneAddress();
            address.setAddress(element.getAsJsonObject().get("text").getAsString());
            list.add(address);
        }
        return list;
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
