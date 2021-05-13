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

        context.analyzer("flat").custom()
                .tokenizer("standard")
                .tokenFilters("asciifolding", "lowercase");

        context.analyzer("location").custom()
                .tokenizer("standard") // edge_ngram
                .tokenFilters("asciifolding", "lowercase", "ngram");//, "truncate", "edge_ngram_filter"); // , "porter_stem" "edge_ngram"

        context.analyzer("streetType").custom()
                .tokenizer("standard") // edge_ngram
                .tokenFilters("asciifolding", "lowercase");

        context.analyzer("suburb").custom()
                .tokenizer("standard") // edge_ngram
                .tokenFilters("asciifolding", "lowercase", "ngram");

        context.normalizer("sort").custom()
                .tokenFilters("asciifolding", "lowercase");

        context.analyzer("address").custom()
                .tokenizer("whitespace") // edge_ngram
                .tokenFilters("asciifolding", "lowercase", "ngram");

        context.analyzer("whitespace").custom()
                .tokenizer("whitespace") // edge_ngram
                .tokenFilters("asciifolding", "lowercase");

        context.tokenFilter("ngram")
                .type("ngram")
                .param("max_gram", 6)
                .param("min_gram", 2);
    }
}
