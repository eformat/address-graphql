package org.acme.service;

import io.quarkus.runtime.StartupEvent;
import org.acme.entity.Address;
import org.acme.entity.StreetType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;
import org.hibernate.search.backend.elasticsearch.ElasticsearchExtension;
import org.hibernate.search.engine.search.common.BooleanOperator;
import org.hibernate.search.mapper.orm.session.SearchSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;
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

    @Query
    @Description("Search addresses by street number, name, type, suburb")
    public List<Address> addresses(@Name("search") String search, @Name("size") Optional<Integer> size) {
        Pattern address = Pattern.compile("^(\\d+)?\\s?(.*)$"); // digit, optional space, rest words
        Matcher matchAddress = address.matcher(search);
        String num = new String();
        String rest = new String();
        if (matchAddress.find()) {
            num = matchAddress.group(1);
            rest = matchAddress.group(2);
        }
        // check if rest words matches street types
        String[] parts = {"", ""};
        String finalStreet = StreetType.instance().matches(search);
        if (finalStreet != null && !finalStreet.trim().isEmpty()) {
            // andrew campbell drive
            parts = rest.split(finalStreet, -1);
        } else {
            parts[0] = rest;
        }
        String finalNum = (num != null ? num : new String());
        String finalLoc = (parts[0] != null ? parts[0] : new String());
        String finalSuburb = (parts[1] != null ? parts[1] : new String());
        log.debug(">>> Final Words: " + finalNum + " " + finalLoc + " " + finalStreet + " " + finalSuburb);

        return searchSession.search(Address.class)
                .extension(ElasticsearchExtension.get())
                .where(f -> ((finalNum == null || finalNum.trim().isEmpty()) && (finalLoc == null || finalLoc.trim().isEmpty()) && (finalStreet == null || finalStreet.trim().isEmpty()) && (finalSuburb == null || finalSuburb.trim().isEmpty())) ?
                        f.matchAll() // search all if all empty
                        : ((finalNum == null || finalNum.trim().isEmpty()) && (finalStreet == null || finalStreet.trim().isEmpty()) && (finalSuburb == null || finalSuburb.trim().isEmpty()) && (finalLoc != null || !finalLoc.trim().isEmpty())) ? // search by location only
                        f.simpleQueryString()
                                .field("street_name")
                                .matching(finalLoc.toLowerCase().trim() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum != null || !finalNum.trim().isEmpty()) && (finalLoc == null || finalLoc.trim().isEmpty()) && (finalStreet == null || finalStreet.trim().isEmpty()) && (finalSuburb == null || finalSuburb.trim().isEmpty())) ? // search by number only
                        f.simpleQueryString()
                                .field("number_first")
                                .matching(finalNum.toLowerCase() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum == null || finalNum.trim().isEmpty()) && (finalLoc == null || finalLoc.trim().isEmpty()) && (finalStreet != null || !finalStreet.trim().isEmpty()) && (finalSuburb == null || finalSuburb.trim().isEmpty())) ? // search by street type only
                        f.simpleQueryString()
                                .field("street_type_code")
                                .matching(finalStreet.toLowerCase() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum == null || finalNum.trim().isEmpty()) && (finalLoc == null || finalLoc.trim().isEmpty()) && (finalStreet == null || finalStreet.trim().isEmpty()) && (finalSuburb != null || !finalSuburb.trim().isEmpty())) ? // search by suburb only
                        f.simpleQueryString()
                                .field("locality_name")
                                .matching(finalStreet.toLowerCase() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum != null || !finalNum.trim().isEmpty()) && (finalLoc != null || !finalLoc.trim().isEmpty()) && (finalStreet != null || !finalStreet.trim().isEmpty()) && (finalSuburb == null || finalSuburb.trim().isEmpty())) ? // search by number, street name, street type
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f) // boost location score
                                        .matching(finalNum + "*")) // wildcard predicate
                                .must(f.simpleQueryString()
                                        .field("street_name") //.boost(2.0f)// boost location score
                                        .matching(finalLoc.toLowerCase().trim() + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                                )
                                .must(f.simpleQueryString()
                                        .field("street_type_code")
                                        .matching(finalStreet + "*")) // wildcard predicate
                        : // search everything ~logical AND
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f) // boost location score
                                        .matching(finalNum + "*")) // wildcard predicate
                                .must(f.simpleQueryString()
                                        .field("street_name") //.boost(2.0f)// boost location score
                                        .matching(finalLoc.toLowerCase().trim() + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                                )
                                .must(f.simpleQueryString()
                                        .field("street_type_code")
                                        .matching(finalStreet + "*")) // wildcard predicate
                                .must(f.simpleQueryString()
                                        .field("locality_name")
                                        .matching(finalSuburb + "*")) // wildcard predicate
                )
                .sort(f -> f.field("number_first_sort").then().field("street_name_sort"))
                .fetchHits(size.orElse(20));
    }
}
