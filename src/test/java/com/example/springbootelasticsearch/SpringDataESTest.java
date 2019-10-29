package com.example.springbootelasticsearch;

import com.example.springbootelasticsearch.es.MyArticle;
import com.example.springbootelasticsearch.repository.MyArticleRepository;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringDataESTest {

    @Autowired
    private MyArticleRepository articleRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    //创建一个索引库
    @Test
    public void createIndex() {
        //创建索引，并配置映射关系
        elasticsearchTemplate.createIndex(MyArticle.class);
        //配置映射
        //elasticsearchTemplate.putMapping(MyArticle.class);
    }

    //添加文档
    @Test
    public void addDocument() {
        MyArticle myArticle = new MyArticle();
        myArticle.setId(1L);
        myArticle.setTitle("hhhhh");
        myArticle.setContent("hhhhhhhh");
        //将文档写入索引库
        articleRepository.save(myArticle);
    }

    //删除文档
    @Test
    public void deleteDocument() {
        articleRepository.deleteById(1L);
    }

    //更新文档
    @Test
    public void updateDocument() {
        MyArticle myArticle = new MyArticle();
        myArticle.setId(1L);
        myArticle.setTitle("ggggggg");
        myArticle.setContent("gggggggggggg");
        //将文档写入索引库
        articleRepository.save(myArticle);
    }

    //查询全部文档
    @Test
    public void query() {
        Iterable<MyArticle> myArticleIterable = articleRepository.findAll();
        myArticleIterable.forEach(myArticle-> System.out.println(myArticle));
    }

    //自定义查询
    @Test
    public void customQuery() {
        //默认会进行分页处理，只显示10条数据
        //查询时会先进行分词再进行搜索，每个词之间是and的关系
        articleRepository.findByTitleOrContent("maven","商务与投资")
                .forEach(myArticle -> System.out.println(myArticle));
    }

    //自定义查询之分页查询
    @Test
    public void customSearchPage() {
        //分页默认0是第一页
        PageRequest pageRequest = PageRequest.of(0, 15);
        //查询时会先进行分词再进行搜索，每个词之间是and的关系
        articleRepository.findByTitleOrContent("maven","商务与投资",pageRequest)
                .forEach(myArticle -> System.out.println(myArticle));
    }

    //使用NativeSearchQuery对象原生查询
    @Test
    public void rawQuery() {
        //创建一个查询对象
        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.queryStringQuery("maven是一个工程构建工具").defaultField("title"))
                .withPageable(PageRequest.of(0, 15))
                .build();
        //执行查询
        List<MyArticle> myArticleList = elasticsearchTemplate.queryForList(query, MyArticle.class);
        myArticleList.forEach(myArticle -> System.out.println(myArticle));
    }
}
