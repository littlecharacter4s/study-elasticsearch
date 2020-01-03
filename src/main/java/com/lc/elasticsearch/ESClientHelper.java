package com.lc.elasticsearch;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class ESClientHelper {
    RestHighLevelClient client;

    private ESClientHelper() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("vps1", 9201, "http"),
                        new HttpHost("vps1", 9202, "http"),
                        new HttpHost("vps1", 9203, "http")
                )
        );
    }

    private static class ESClientHelperInner {
        private static final ESClientHelper helper = new ESClientHelper();

        private ESClientHelperInner() {
        }
    }

    public static ESClientHelper getInstance() {
        return ESClientHelperInner.helper;
    }

    public void createIndex(String index) {
        try {

            // 1、创建 创建索引request 参数：索引名mess
            CreateIndexRequest request = new CreateIndexRequest(index);

            // 2、设置索引的settings
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 3) // 分片数
                    .put("index.number_of_replicas", 2) // 副本数
            );

            // 3、设置索引的mappings，对应mysql表结构
            /*
             字段的数据类型:
                简单类型
                    text
                    date
                    integer/long/floating
                    boolean
                    ip4&ip6
                    keyword
                复杂类型
                    对象类型
                    嵌套类型
                特殊类型（地理信息）
                    geo_point&geo_shape、percolator
             */
            request.mapping("{\n" +
                            "    \"properties\" : {\n" +
                            "        \"message\" : {\n" +
                            "            \"type\" : \"text\"\n" +
                            "        },\n" +
                            "        \"user\" : {\n" +
                            "            \"type\" : \"text\"\n" +
                            "        },\n" +
                            "        \"post_date\" : {\n" +
                            "            \"type\" : \"text\"\n" +
                            "        },\n" +
                            "        \"age\" : {\n" +
                            "            \"type\" : \"integer\"\n" +
                            "        }\n" +
                            "    }\n" +
                            "}",
                    XContentType.JSON);

            // 4、 设置索引的别名
            request.alias(new Alias("mmo"));

            // 5、 发送请求
            // 5.1 同步方式发送请求
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

            // 6、处理响应
            boolean acknowledged = createIndexResponse.isAcknowledged();
            boolean shardsAcknowledged = createIndexResponse
                    .isShardsAcknowledged();
            System.out.println("acknowledged = " + acknowledged);
            System.out.println("shardsAcknowledged = " + shardsAcknowledged);

            // 5.1 异步方式发送请求
            /*ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(
                        CreateIndexResponse createIndexResponse) {
                    // 6、处理响应
                    boolean acknowledged = createIndexResponse.isAcknowledged();
                    boolean shardsAcknowledged = createIndexResponse
                            .isShardsAcknowledged();
                    System.out.println("acknowledged = " + acknowledged);
                    System.out.println(
                            "shardsAcknowledged = " + shardsAcknowledged);
                }

                @Override
                public void onFailure(Exception e) {
                    System.out.println("创建索引异常：" + e.getMessage());
                }
            };

            client.indices().createAsync(request, listener);
            */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addDocument(String indexName, String docId) {
        try {
            // 1、创建索引请求
            // IndexRequest request = new IndexRequest("11", "111", "111");
            IndexRequest request = new IndexRequest(indexName);
            request.id(docId);

            // 2、准备文档数据
            // 方式一：直接给JSON串
            String jsonString = "{" +
                    "\"user\":\"lisi\"," +
                    "\"post_date\":\"2018-01-30\"," +
                    "\"message\":\"zhangsan shi ge sb\"," +
                    "\"age\":18" +
                    "}";
            request.source(jsonString, XContentType.JSON);

            // 方式二：以map对象来表示文档
            /*
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("user", "kimchy");
            jsonMap.put("postDate", new Date());
            jsonMap.put("message", "trying out Elasticsearch");
            request.source(jsonMap);
            */

            // 方式三：用XContentBuilder来构建文档
            /*
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy");
                builder.field("postDate", new Date());
                builder.field("message", "trying out Elasticsearch");
            }
            builder.endObject();
            request.source(builder);
            */

            // 方式四：直接用key-value对给出
            /*
            request.source("user", "kimchy",
                            "postDate", new Date(),
                            "message", "trying out Elasticsearch");
            */

            //3、其他的一些可选设置
            /*
            request.routing("routing");  //设置routing值
            request.timeout(TimeValue.timeValueSeconds(1));  //设置主分片等待时长
            request.setRefreshPolicy("wait_for");  //设置重刷新策略
            request.version(2);  //设置版本号
            request.opType(DocWriteRequest.OpType.CREATE);  //操作类别
            */

            //4、发送请求
            // 同步方式
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

            //5、处理响应
            if (indexResponse != null) {
                String index = indexResponse.getIndex();
                String id = indexResponse.getId();
                long version = indexResponse.getVersion();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    System.out.println("新增文档成功，处理逻辑代码写到这里。");
                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    System.out.println("修改文档成功，处理逻辑代码写到这里。");
                }
                // 分片处理信息
                ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
                System.out.println(JSON.toJSONString(shardInfo));
                // 如果有分片副本失败，可以获得失败原因信息
                if (shardInfo.getFailed() > 0) {
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        System.out.println("副本失败原因：" + reason);
                    }
                }
            }

            //异步方式发送索引请求
            /*ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {

                }

                @Override
                public void onFailure(Exception e) {

                }
            };
            client.indexAsync(request, listener);
            */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void queryDocument(String searchIndex, String docId) {
        try {
            // 1、创建获取文档请求
            GetRequest request = new GetRequest(searchIndex, docId);

            // 2、可选的设置
            //request.routing("routing");
            //request.version(2);
            //是否获取_source字段
            //request.fetchSourceContext(new FetchSourceContext(false));
            //选择返回的字段
            String[] includes = new String[]{"user", "*date"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);

            // 取stored字段
            /*request.storedFields("message");
            GetResponse getResponse = client.get(request);
            String message = getResponse.getField("message").getValue();*/

            //3、发送请求
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);

            //4、处理响应
            if (getResponse != null) {
                String index = getResponse.getIndex();
                String type = getResponse.getType();
                String id = getResponse.getId();
                if (getResponse.isExists()) {
                    // 文档存在
                    long version = getResponse.getVersion();
                    //结果取成 String
                    String sourceAsString = getResponse.getSourceAsString();
                    // 结果取成Map
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    //结果取成字节数组
                    byte[] sourceAsBytes = getResponse.getSourceAsBytes();

                    System.out.println("index:" + index + "  type:" + type + "  id:" + id);
                    System.out.println(sourceAsString);
                } else {
                    System.out.println("没有找到该id的文档");
                }
            }


            //异步方式发送获取文档请求
            /*
            ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse getResponse) {

                }

                @Override
                public void onFailure(Exception e) {

                }
            };
            client.getAsync(request, listener);
            */

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void searchDocument(String searchIndex) {
        try {
            // 1、创建search请求
            //SearchRequest searchRequest = new SearchRequest();
            SearchRequest searchRequest = new SearchRequest(searchIndex);

            // 2、用SearchSourceBuilder来构造查询请求体 ,请仔细查看它的方法，构造各种查询的方法都在这。
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            //构造QueryBuilder
            /*QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "kimchy")
                    .fuzziness(Fuzziness.AUTO)
                    .prefixLength(3)
                    .maxExpansions(10);
            sourceBuilder.query(matchQueryBuilder);*/

            sourceBuilder.query(QueryBuilders.termQuery("user", "zhangsan"));
            sourceBuilder.from(0);
            sourceBuilder.size(10);
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

            //是否返回_source字段
            //sourceBuilder.fetchSource(false);

            //设置返回哪些字段
            /*String[] includeFields = new String[] {"title", "user", "innerObject.*"};
            String[] excludeFields = new String[] {"_type"};
            sourceBuilder.fetchSource(includeFields, excludeFields);*/

            //指定排序
            //sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
            //sourceBuilder.sort(new FieldSortBuilder("_uid").order(SortOrder.ASC));

            // 设置返回 profile
            //sourceBuilder.profile(true);

            //将请求体加入到请求中
            searchRequest.source(sourceBuilder);

            // 可选的设置
            //searchRequest.routing("routing");

            // 高亮设置
            /*
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field highlightTitle =
                    new HighlightBuilder.Field("title");
            highlightTitle.highlighterType("unified");
            highlightBuilder.field(highlightTitle);
            HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
            highlightBuilder.field(highlightUser);
            sourceBuilder.highlighter(highlightBuilder);*/


            //加入聚合
            /*TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
                    .field("company.keyword");
            aggregation.subAggregation(AggregationBuilders.avg("average_age")
                    .field("age"));
            sourceBuilder.aggregation(aggregation);*/

            //做查询建议
            /*SuggestionBuilder termSuggestionBuilder =
                    SuggestBuilders.termSuggestion("user").text("kmichy");
                SuggestBuilder suggestBuilder = new SuggestBuilder();
                suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
            sourceBuilder.suggest(suggestBuilder);*/

            //3、发送请求
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            System.out.println(JSON.toJSONString(searchResponse));

            //4、处理响应
            //搜索结果状态信息
            RestStatus status = searchResponse.status();
            TimeValue took = searchResponse.getTook();
            Boolean terminatedEarly = searchResponse.isTerminatedEarly();
            boolean timedOut = searchResponse.isTimedOut();

            //分片搜索情况
            int totalShards = searchResponse.getTotalShards();
            int successfulShards = searchResponse.getSuccessfulShards();
            int failedShards = searchResponse.getFailedShards();
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                // failures should be handled here
            }

            //处理搜索命中文档结果
            SearchHits hits = searchResponse.getHits();

            long totalHits = hits.getTotalHits().value;
            float maxScore = hits.getMaxScore();

            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                // do something with the SearchHit

                String index = hit.getIndex();
                //String type = hit.getType();
                String id = hit.getId();
                float score = hit.getScore();

                //取_source字段值
                String sourceAsString = hit.getSourceAsString(); //取成json串
                Map<String, Object> sourceAsMap = hit.getSourceAsMap(); // 取成map对象
                //从map中取字段值
                /*
                String documentTitle = (String) sourceAsMap.get("title");
                List<Object> users = (List<Object>) sourceAsMap.get("user");
                Map<String, Object> innerObject = (Map<String, Object>) sourceAsMap.get("innerObject");
                */
                System.out.println("index:" + index + "  id:" + id);
                System.out.println(sourceAsString);

                //取高亮结果
                /*Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get("title");
                Text[] fragments = highlight.fragments();
                String fragmentString = fragments[0].string();*/
            }

            // 获取聚合结果
            /*
            Aggregations aggregations = searchResponse.getAggregations();
            Terms byCompanyAggregation = aggregations.get("by_company");
            Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
            Avg averageAge = elasticBucket.getAggregations().get("average_age");
            double avg = averageAge.getValue();
            */

            // 获取建议结果
            /*Suggest suggest = searchResponse.getSuggest();
            TermSuggestion termSuggestion = suggest.getSuggestion("suggest_user");
            for (TermSuggestion.Entry entry : termSuggestion.getEntries()) {
                for (TermSuggestion.Entry.Option option : entry) {
                    String suggestText = option.getText().string();
                }
            }
            */

            //异步方式发送获查询请求
            /*
            ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse getResponse) {
                    //结果获取
                }

                @Override
                public void onFailure(Exception e) {
                    //失败处理
                }
            };
            client.searchAsync(searchRequest, listener);
            */

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateDocument(String indexName, String docId) {
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, docId);
            updateRequest.doc("{" +
                    "\"user\":\"zhangsan\"," +
                    "\"post_date\":\"2013-01-30\"," +
                    "\"message\":\"lisi shi ge sb\"" +
                    "}", XContentType.JSON);
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            System.out.println(JSON.toJSONString(updateResponse));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteDocument(String indexName, String docId) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(indexName, docId);
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println(JSON.toJSONString(deleteResponse));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteIndex(String indexName) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);//指定要删除的索引名称
            //可选参数：
            request.timeout(TimeValue.timeValueMinutes(2)); //设置超时，等待所有节点确认索引删除（使用TimeValue形式）
            // request.timeout("2m"); //设置超时，等待所有节点确认索引删除（使用字符串形式）

            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));////连接master节点的超时时间(使用TimeValue方式)
            // request.masterNodeTimeout("1m");//连接master节点的超时时间(使用字符串方式)

            //设置IndicesOptions控制如何解决不可用的索引以及如何扩展通配符表达式
            request.indicesOptions(IndicesOptions.lenientExpandOpen());

            //同步执行
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);

            System.out.println(JSON.toJSONString(deleteIndexResponse));

            /* //异步执行删除索引请求需要将DeleteIndexRequest实例和ActionListener实例传递给异步方法：
            //DeleteIndexResponse的典型监听器如下所示：
            //异步方法不会阻塞并立即返回。
            ActionListener<DeleteIndexResponse> listener = new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                    //如果执行成功，则调用onResponse方法;
                }

                @Override
                public void onFailure(Exception e) {
                    //如果失败，则调用onFailure方法。
                }
            };
            client.indices().deleteAsync(request, listener);*/

            //Delete Index Response
            //返回的DeleteIndexResponse允许检索有关执行的操作的信息，如下所示：
            boolean acknowledged = deleteIndexResponse.isAcknowledged();//是否所有节点都已确认请求

            if (!acknowledged) {
                //如果找不到索引，则会抛出ElasticsearchException：
                request = new DeleteIndexRequest("does_not_exist");
                client.indices().delete(request, RequestOptions.DEFAULT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void scroll() {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L)); //设定滚动时间间隔
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchAllQuery());
        searchSourceBuilder.size(5); //设定每次返回多少条数据
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        System.out.println("-----首页-----");
        for (SearchHit searchHit : searchHits) {
            System.out.println(searchHit.getSourceAsString());
        }
        //遍历搜索命中的数据，直到没有数据
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            try {
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                System.out.println("-----下一页-----");
                for (SearchHit searchHit : searchHits) {
                    System.out.println(searchHit.getSourceAsString());
                }
            }

        }
        //清除滚屏
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);//也可以选择setScrollIds()将多个scrollId一起使用
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        boolean succeeded = clearScrollResponse.isSucceeded();
        System.out.println("succeeded:" + succeeded);
    }

    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
