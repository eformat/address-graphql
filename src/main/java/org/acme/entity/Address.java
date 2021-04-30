package org.acme.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.FullTextField;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.KeywordField;

import javax.persistence.Entity;
import java.util.Objects;

@Entity
@Indexed
public class Address extends PanacheEntity {

    // premises elements
    public String subBuildingName;
    public String buildingName;

    @FullTextField(analyzer = "number")
    @KeywordField(name = "buildingNumber_sort", sortable = Sortable.YES, normalizer = "sort")
    public String buildingNumber;
    public String organisation;
    public String department;
    public String poBoxNumber;

    // throughfare elements
    public String throughfare;
    public String dependentThroughfare;

    // locality elements
    public String doubleDependentLocality;

    @FullTextField(analyzer = "location")
    @KeywordField(name = "dependentLocality_sort", sortable = Sortable.YES, normalizer = "sort")
    public String dependentLocality;
    public String postTown;

    // postcode element
    public String postcode;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Address)) {
            return false;
        }
        Address other = (Address) o;
        return Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() {
        return 31;
    }
}

/*
// https://github.com/steinfletcher/paf-address-format/blob/master/src/test/java/com/steinf/pafaddressformat/DeliveryPointTest.java

    DeliveryPoint deliveryPoint = new DeliveryPoint.Builder()
        .withSubBuildingName("FLAT 1")
        .withBuildingName("VICTORIA HOUSE")
        .withBuildingNumber("15")
        .withDependentLocality("COOMBE BISSETT")
        .withOrganisation("SURE FIT COVERS")
        .withDepartment("EMERGENCY")
        .withPoBoxNumber("1242")
        .withDoubleDependentLocality("TYRE INDUSTRIAL ESTATE")
        .withDependentThroughfare("CHESHUNT MEWS")
        .withThroughfare("CYPRESS STREET")
        .withPostcode("CV3 3GU")
        .withPostTown("COVENTRY")
        .build();
 */

