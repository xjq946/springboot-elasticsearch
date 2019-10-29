package com.example.springbootelasticsearch;

import com.example.springbootelasticsearch.es.Article;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

public class ElasticsearchClientTest {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        //创建一个Settings对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
    }

    //使用Java客户端创建索引库
    @Test
    public void createIndex() throws UnknownHostException {
        //创建一个Settings对象，相当于是一个配置信息，主要配置集群的名称
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        //创建一个客户端Client对象
        PreBuiltTransportClient client = new PreBuiltTransportClient(settings);
        //指定集群的节点列表，端口号使用9300，可以指定任意一个节点地址，但是一般都指定多个确保高可用
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        //使用client对象创建一个索引库
        //admin()表示使用管理员权限创建
        client.admin().indices().prepareCreate("blog")
                //执行操作
                .get();
        //关闭client对象
        client.close();
    }

    //使用Java客户端设置mapping映射
    @Test
    public void setMappings() throws IOException {
        //创建一个Settings对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        //创建一个Mappings信息
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject() //相当于“{”
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type","long")
                                .field("store",true)
                            .endObject()
                            .startObject("title")
                                .field("type","text")
                                .field("store",true)
                                .field("analyzer","ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type","text")
                                .field("store",true)
                                .field("analyzer","ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        //使用client对象把mapping信息设置到索引库中
        client.admin().indices()
                //设置要做映射的索引
                .preparePutMapping("blog")
                //设置要做映射的type
                .setType("article")
                //mapping信息，可以是XContentBuilder对象可以是json格式的字符串
                .setSource(builder)
                //执行操作
                .get();
        //关闭client
        client.close();
    }

    //方式一：使用Java客户端向索引库中添加文档
    @Test
    public void testAddDocument() throws Exception {
        //创建一个client对象
        //创建一个文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",1L)
                    .field("title","hhhhhhhhhh")
                    .field("content","hhhhhhhhhhhhh")
                .endObject();
        //把文档对象添加到索引库中
        //方式一：client.prepareIndex("blog","article","1");
        //方式二
        client.prepareIndex()
                //设置索引名称
                .setIndex("blog")
                //设置type
                .setType("article")
                //设置文档的id，如果不设置的话会自动生成一个id
                .setId("1")
                //设置文档信息
                .setSource(builder)
                //执行操作
                .get();
        //关闭客户端
        client.close();
    }

    //方式二：使用Java客户端向索引库中添加文档
    @Test
    public void testAddDocument2() throws JsonProcessingException {
        //创建一个Article对象
        Article article = new Article();
        //设置对象的属性
        article.setId(1L);
        article.setTitle("hhhhhhhhhh");
        article.setContent("hhhhhhhhhhh");
        //把article对象转换成json格式的字符串
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonDocument = objectMapper.writeValueAsString(article);
        //使用client对象把文档写入索引库
        client.prepareIndex("blog","article","1")
                .setSource(jsonDocument, XContentType.JSON)
                .get();
        //关闭客户端
        client.close();
    }

    private void searchBuilder(QueryBuilder queryBuilder) {
        //使用client执行查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(queryBuilder)
                .get();
        //得到查询的结果
        SearchHits searchHits = searchResponse.getHits();
        //取查询结果的总记录数
        long totalHits = searchHits.getTotalHits();
        //获取查询结果列表
        Iterator<SearchHit> iterator = searchHits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            //打印文档对象，以json格式输出
            System.out.println(searchHit.getSourceAsString());
            //取文档的属性
            Map<String, Object> document = searchHit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }
        //关闭client
        client.close();
    }

    //使用Java客户端根据id查询
    @Test
    public void testQueryById() {
        //创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder对象
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "2");
        searchBuilder(queryBuilder);
    }

    //使用Java客户端根据关键词查询
    @Test
    public void testTermQuery() throws UnknownHostException {
        //创建一个Settings对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        //设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("articles")
                .setQuery(QueryBuilders.termQuery("content", "搜索"))
                .get();
        //遍历搜索结果
        SearchHits hits = searchResponse.getHits();
        //获取命中次数
        long totalHits = hits.getTotalHits();
        //获取查询结果
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            //获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
            System.out.println("title:"+searchHit.getSource().get("title"));
        }
        //释放资源
        client.close();
    }

    //使用Java客户端根据queryString查询
    @Test
    public void testQueryString() {
        //如果不指定默认域，默认在所有域中进行查询
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("速度与激情").defaultField("title");
        searchBuilder(queryBuilder);
    }

    //使用Java客户端分页查询
    @Test
    public void testSearchPage() throws UnknownHostException {
        //如果不设置分页，默认只返回10条数据
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("女护士").defaultField("title");
        //创建一个Settings对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        //设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("articles")
                .setQuery(QueryBuilders.termQuery("content", "搜索"))
                //设置分页信息
                .setFrom(0)
                //每页显示的行数
                .setSize(5)
                .get();
        //遍历搜索结果
        SearchHits hits = searchResponse.getHits();
        //获取命中次数
        long totalHits = hits.getTotalHits();
        //获取查询结果
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            //获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
            System.out.println("title:"+searchHit.getSource().get("title"));
        }
        //释放资源
        client.close();
    }

    //使用Java客户端查询结果高亮显示
    @Test
    public void testHighlight() throws UnknownHostException {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("女护士").defaultField("title");
        //创建一个Settings对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        //执行查询之前设置高亮信息
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //设置高亮显示的字段
        highlightBuilder.field("title");
        //设置高亮显示的前缀
        highlightBuilder.preTags("<em>");
        //设置高亮显示的后缀
        highlightBuilder.postTags("</em>");
        //设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("articles")
                .setQuery(QueryBuilders.termQuery("content", "搜索"))
                //设置分页信息
                .setFrom(0)
                //每页显示的行数
                .setSize(5)
                //设置高亮信息
                .highlighter(highlightBuilder)
                .get();
        //遍历搜索结果
        SearchHits hits = searchResponse.getHits();
        //获取命中次数
        long totalHits = hits.getTotalHits();
        //获取查询结果
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next();
            //获取字符串格式打印
            System.out.println(searchHit.getSourceAsString());
            System.out.println("title:"+searchHit.getSource().get("title"));
            //获取高亮结果
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println(highlightFields);
            //获取title高亮显示的结果
            HighlightField field = highlightFields.get("title");
            Text[] fragments = field.getFragments();
            if (!CollectionUtils.isEmpty(fragments)) {
                System.out.println(fragments[0].toString());
            }
        }
        //释放资源
        client.close();
    }
}
