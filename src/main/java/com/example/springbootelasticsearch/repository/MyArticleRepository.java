package com.example.springbootelasticsearch.repository;

import com.example.springbootelasticsearch.es.MyArticle;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface MyArticleRepository extends ElasticsearchRepository<MyArticle,Long> {
    //自定义查询
    List<MyArticle> findByTitleOrContent(String title,String content);
    //自定义查询之分页查询
    List<MyArticle> findByTitleOrContent(String title, String content, Pageable pageable);
}
