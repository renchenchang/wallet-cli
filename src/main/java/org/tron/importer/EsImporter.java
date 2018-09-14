package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.tron.common.crypto.Sha256Hash;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Protocol.Block;
import org.tron.walletserver.WalletApi;


public class EsImporter {

  private static RestHighLevelClient client = new RestHighLevelClient(
      RestClient.builder(
          new HttpHost("18.223.114.116", 9200, "http")
      ));

  private static byte[] getBlockID(Block block) {
    long blockNum = block.getBlockHeader().getRawData().getNumber();
    byte[] blockHash = Sha256Hash.of(block.getBlockHeader().getRawData().toByteArray())
        .getByteString().toByteArray();
    byte[] numBytes = Longs.toByteArray(blockNum);
    byte[] hash = new byte[blockHash.length];
    System.arraycopy(numBytes, 0, hash, 0, 8);
    System.arraycopy(blockHash, 8, hash, 8, blockHash.length - 8);
    return hash;
  }

  public static void loadDataFromNode(RestHighLevelClient esClient) throws IOException {
    ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
      @Override
      public void onResponse(IndexResponse indexResponse) {
        System.out.println(indexResponse.toString());
      }

      @Override
      public void onFailure(Exception e) {
        System.out.println(e.getMessage());
      }
    };

    for (int i = 0; i < 10; i++) {
      Block block = WalletApi.getBlock4Loader(i, true);
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      {
        builder.field("number", block.getBlockHeader().getRawData().getNumber());
        builder.field("witness", WalletApi
            .encode58Check(block.getBlockHeader().getRawData().getWitnessAddress().toByteArray()));
        builder.field("parent",
            ByteArray
                .toHexString(block.getBlockHeader().getRawData().getParentHash().toByteArray()));

      }
      builder.endObject();

      IndexRequest indexRequest = new IndexRequest("tron", "blocks",
          ByteArray.toHexString(getBlockID(block)))
          .source(builder);
      // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
      esClient.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
    }
  }

  public static void getByid(String index, String type, String id) throws IOException {
    GetRequest getRequest = new GetRequest(index, type, id);
    try {
      GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
      getResponse.getSource().entrySet().stream()
          .forEach(x -> System.out.println(x.getKey() + " ->" + x.getValue()));
    } catch (ElasticsearchException e) {
      System.out.println(e.getMessage());
    }
  }

  public static String getMaxBlockNumberInES() throws IOException {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices("tron");
    searchRequest.types("blocks");
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.sort(new FieldSortBuilder("number").order(SortOrder.DESC));
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    return searchResponse.getHits().getAt(0).getSourceAsMap().get("number").toString();
  }


  public static void main(String[] args) throws IOException, SQLException {

    //  loadDataFromNode(client);

    JSONObject query = JSONObject.parseObject("{\n"
        + "\"query\": \"select max(number) from tron \"\n"
        + "}");

//    String result = Http.doPost("http://18.223.114.116:9200/_xpack/sql?format=txt", query.toJSONString(), "utf-8");
//    System.out.println(result);

    String address = "jdbc:es://18.223.114.116:9200";
    try {
      Properties connectionProperties = new Properties();
      Connection connection = DriverManager.getConnection(address, connectionProperties);
      Statement statement = connection.createStatement();
      ResultSet results = statement.executeQuery("select max(number) from tron");
      while (results.next()) {
        System.out.println(results.getInt(1));
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      client.close();
    }
  }
}
