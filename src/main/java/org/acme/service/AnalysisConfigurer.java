package org.acme.service;

import org.hibernate.search.backend.elasticsearch.analysis.ElasticsearchAnalysisConfigurationContext;
import org.hibernate.search.backend.elasticsearch.analysis.ElasticsearchAnalysisConfigurer;

import javax.enterprise.context.Dependent;
import javax.inject.Named;

@Dependent
@Named("myAnalysisConfigurer")
public class AnalysisConfigurer implements ElasticsearchAnalysisConfigurer {

    @Override
    public void configure(ElasticsearchAnalysisConfigurationContext context) {

        context.analyzer("number").custom()
                .tokenizer("standard")
                .tokenFilters("asciifolding", "lowercase", "porter_stem");

        context.analyzer("location").custom()
                .tokenizer("standard")
                .tokenFilters("asciifolding", "lowercase", "porter_stem");

        context.normalizer("sort").custom()
                .tokenFilters("asciifolding", "lowercase");
    }
}
