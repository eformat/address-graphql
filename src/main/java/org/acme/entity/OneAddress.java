package org.acme.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import org.hibernate.search.mapper.pojo.bridge.mapping.annotation.ValueBinderRef;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.FullTextField;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.NonStandardField;

import javax.persistence.Entity;

@Entity
@Indexed
public class OneAddress extends PanacheEntity {

    @FullTextField(analyzer = "address")
    @NonStandardField(name = "address_completion", valueBinder = @ValueBinderRef(type = CompletionBinder.class))
    public String address;
}
