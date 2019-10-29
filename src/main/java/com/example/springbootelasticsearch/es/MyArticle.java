package com.example.springbootelasticsearch.es;

import lombok.Data;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Data
@Document(indexName = "blog",type = "article")
public class MyArticle  {

    @Id
    @Field(type = FieldType.Long,store = true)
    private long id;

    @Field(type = FieldType.Text,store = true,analyzer = "ik_smart")
    private String title;

    @Field(type = FieldType.Text,store = true,analyzer = "ik_smart")
    private String content;
}
