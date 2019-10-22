package com.example.springbootelasticsearch;

import com.example.springbootelasticsearch.es.Item;
import com.example.springbootelasticsearch.repository.ItemRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
class SpringbootElasticsearchApplicationTests {

    //普通的增删改查不用这个类，复杂查询才会用到
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    private ItemRepository itemRepository;

    //创建索引库以及创建映射关系
    @Test
    public void contextLoads() {
        //创建索引库
        elasticsearchTemplate.createIndex(Item.class);
        //创建映射关系
        elasticsearchTemplate.putMapping(Item.class);
    }

    //批量新增
    @Test
    public void indexList() {
        List<Item> list = new ArrayList<>();
        list.add(new Item(2L, "坚果手机R1", " 手机", "锤子", 3699.00, "http://image.leyou.com/123.jpg"));
        list.add(new Item(3L, "华为META10", " 手机", "华为", 4499.00, "http://image.leyou.com/3.jpg"));
        // 接收对象集合，实现批量新增
        itemRepository.saveAll(list);
    }

    //测试查询
    @Test
    public void testFind() {
        Iterable<Item> all = itemRepository.findAll();
        for(Item item : all) {
            System.out.println("item = "+ item);
        }
    }

    //自定义查询
    @Test
    public void testFindBy() {
        List<Item> items = itemRepository.findByPriceBetween(2000d, 4000d);
        for(Item item : items) {
            System.out.println("item = "+ item);
        }
    }

    //分页查询
    @Test
    public void testQuery() {
        //创建查询构造器，用于创建SearchQuery
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //添加结果过滤条件,即挑选返回字段
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{"id","title","price"},null));
        //查询条件
        queryBuilder.withQuery(QueryBuilders.matchQuery("tilte","小米手机"));
        //排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        //分页
        queryBuilder.withPageable(PageRequest.of(1,2));

        Page<Item> result = itemRepository.search(queryBuilder.build());

        //获取总记录数
        long total = result.getTotalElements();
        System.out.println(total);
        //获取总页数
        int totalPages = result.getTotalPages();
        System.out.println(totalPages);
        //获取记录列表数据
        List<Item> list = result.getContent();
        for(Item item : list) {
            System.out.println(item);
        }
    }

    //聚合查询
    @Test
    public void testAggs() {
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //聚合名称
        String aggName = "popularBrand";
        //聚合条件
        queryBuilder.addAggregation(AggregationBuilders.terms(aggName).field("brand"));
        //查询并返回聚合结果
        AggregatedPage<Item> result = elasticsearchTemplate.queryForPage(queryBuilder.build(), Item.class);
        //解析聚合
        Aggregations aggs = result.getAggregations();
        //获取指定名称的聚合
        StringTerms terms = aggs.get(aggName);
        //获取桶，StringTerms是Aggregations的子接口
        List<StringTerms.Bucket> buckets = terms.getBuckets();
        for(StringTerms.Bucket bucket : buckets) {
            //获取key
            String key = bucket.getKeyAsString();
            System.out.println("key:"+key);
            //获取doc_count
            long docCount = bucket.getDocCount();
            System.out.println("docCount:"+docCount);
        }
    }
 }
