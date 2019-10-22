package com.example.springbootelasticsearch.repository;

import com.example.springbootelasticsearch.es.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ItemRepository extends ElasticsearchRepository<Item,Long> {
    //自定义查询
    List<Item> findByPriceBetween(Double begin,Double end);
}
