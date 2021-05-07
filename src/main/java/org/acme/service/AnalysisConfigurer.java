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
                .tokenFilters("asciifolding", "lowercase");

        context.analyzer("location").custom()
                .tokenizer("standard") // edge_ngram
                .tokenFilters("asciifolding", "lowercase");//, "truncate", "edge_ngram_filter"); // , "porter_stem" "edge_ngram"

        context.analyzer("streetType").custom()
                .tokenizer("standard") // edge_ngram
                .tokenFilters("asciifolding", "lowercase");

        context.analyzer("suburb").custom()
                .tokenizer("standard") // edge_ngram
                .tokenFilters("asciifolding", "lowercase");

//        context.tokenFilter("edge_ngram_filter")
//                .type("edgeNGram")
//                .param("max_gram", 15)
//                .param("min_gram", 1);

        context.normalizer("sort").custom()
                .tokenFilters("asciifolding", "lowercase");
    }
}
