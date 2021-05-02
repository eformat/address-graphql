package org.acme.service;

import io.quarkus.runtime.StartupEvent;
import org.acme.entity.Address;
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

    @Transactional
    void onStart(@Observes StartupEvent ev) throws InterruptedException {
        // only reindex if we imported some content
        if (Address.count() > 0) {
            searchSession.massIndexer()
                    .startAndWait();
        }
    }

    @Query
    @Description("Search addresses by street name or number")
    public List<Address> addresses(@Name("search") String search, @Name("size") Optional<Integer> size) {
        Pattern digits = Pattern.compile("([0-9]+)"); // digits
        Matcher matchDigits = digits.matcher(search);
        String num = new String();
        while (matchDigits.find()) {
            num = matchDigits.group(0);
        }
        String finalNum = num;
        log.debug(">>> Final Digits " + finalNum);

        Pattern words = Pattern.compile("([\\D\\s]+)"); // non digits and whitespaces
        Matcher matchWords = words.matcher(search);
        String loc = new String();
        if (matchWords.find()) {
            loc = matchWords.group();
        }
        String finalLoc = loc;
        log.debug(">>> Final Words " + finalLoc);

        return searchSession.search(Address.class)
                .extension(ElasticsearchExtension.get())
                .where(f -> ((finalNum == null || finalNum.trim().isEmpty()) && (finalLoc == null || finalLoc.trim().isEmpty())) ?
                        f.matchAll() // search all if both empty
                        : ((finalNum == null || finalNum.trim().isEmpty()) && (finalLoc != null || !finalLoc.trim().isEmpty())) ? // search by location only
                        f.simpleQueryString()
                                .field("street_name")
                                .matching(finalLoc.toLowerCase() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((finalNum != null || !finalNum.trim().isEmpty()) && (finalLoc == null || finalLoc.trim().isEmpty())) ? // search by number only
                        f.simpleQueryString()
                                .field("number_first")
                                .matching(finalNum.toLowerCase() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : // search by number and location using logical ~OR
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f) // boost location score
                                        .matching(finalNum + "*")) // wildcard predicate
                                .must(f.simpleQueryString()
                                        .field("street_name") //.boost(2.0f)// boost location score
                                        .matching(finalLoc.toLowerCase() + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                                ))
                .sort(f -> f.field("number_first_sort").then().field("street_name_sort"))
                .fetchHits(size.orElse(20));
    }
}
