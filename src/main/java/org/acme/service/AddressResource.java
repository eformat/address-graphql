package org.acme.service;

import io.quarkus.runtime.StartupEvent;
import org.acme.entity.Address;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;
import org.hibernate.search.mapper.orm.session.SearchSession;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.util.List;
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
    @Description("Search addresses by buildingNumber or Locality")
    public List<Address> addresses(@Name("numLoc") String numLoc) {
        Optional<Integer> size = Optional.of(2);
        return searchSession.search(Address.class)
                .where(f ->
                        numLoc == null || numLoc.trim().isEmpty() ?
                                f.matchAll() :
                                f.simpleQueryString()
                                        .fields("buildingNumber", "dependentLocality").matching(numLoc)
                )
                .sort(f -> f.field("buildingNumber_sort").then().field("dependentLocality_sort"))
                .fetchHits(size.orElse(20));
    }
}
