package org.acme.service;

import com.google.gson.JsonObject;
import io.quarkus.runtime.StartupEvent;
import org.acme.entity.Address;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;
import org.hibernate.search.backend.elasticsearch.ElasticsearchExtension;
import org.hibernate.search.engine.search.common.BooleanOperator;
import org.hibernate.search.mapper.orm.Search;
import org.hibernate.search.mapper.orm.session.SearchSession;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@GraphQLApi
public class AddressResource {

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
    public List<Address> addresses(@Name("num") String num, @Name("loc") String loc, @Name("size") Optional<Integer> size) {
        return searchSession.search(Address.class)
                .extension(ElasticsearchExtension.get())
                .where(f -> ((num == null || num.trim().isEmpty()) && (loc == null || loc.trim().isEmpty())) ?
                        f.matchAll() // search all if both empty
                        : ((num == null || num.trim().isEmpty()) && (loc != null || !loc.trim().isEmpty())) ? // search by location only
                        f.simpleQueryString()
                                .field("street_name")
                                .matching(loc.toLowerCase() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : ((num != null || !num.trim().isEmpty()) && (loc == null || loc.trim().isEmpty())) ? // search by number only
                        f.simpleQueryString()
                                .field("number_first")
                                .matching(num.toLowerCase() + "*") // wildcard predicate
                                .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                        : // search by number and location using logical ~OR
                        f.bool()
                                .must(f.simpleQueryString()
                                        .field("number_first").boost(2.0f) // boost location score
                                        .matching(num + "*")) // wildcard predicate
                                .must(f.simpleQueryString()
                                        .field("street_name") //.boost(2.0f)// boost location score
                                        .matching(loc.toLowerCase() + "*") // wildcard predicate
                                        .defaultOperator(BooleanOperator.AND) // default is OR, we want and for wildcard
                                ))
                .sort(f -> f.field("number_first_sort").then().field("street_name_sort"))
                .fetchHits(size.orElse(20));
    }
}
