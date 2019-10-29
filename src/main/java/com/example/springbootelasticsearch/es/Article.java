package com.example.springbootelasticsearch.es;

import lombok.Data;

@Data
public class Article {

    private long id;

    private String title;

    private String content;
}
