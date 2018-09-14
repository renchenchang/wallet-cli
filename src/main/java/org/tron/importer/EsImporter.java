package org.tron.importer;

import com.google.common.primitives.Longs;
import java.io.IOException;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.tron.common.crypto.Sha256Hash;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Protocol.Block;
import org.tron.walletserver.WalletApi;


public class EsImporter {

  private static byte[] getBlockID(Block block) {
    long blockNum = block.getBlockHeader().getRawData().getNumber();
    byte[] blockHash = Sha256Hash.of(block.getBlockHeader().getRawData().toByteArray()).getByteString().toByteArray();
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

    for (int i=0; i<10; i++) {
      Block block = WalletApi.getBlock4Loader(i, true);
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      {
        builder.field("number", block.getBlockHeader().getRawData().getNumber());
        builder.field("witness", WalletApi
            .encode58Check(block.getBlockHeader().getRawData().getWitnessAddress().toByteArray()));
        builder.field("parent",
            ByteArray.toHexString(block.getBlockHeader().getRawData().getParentHash().toByteArray()));
        builder.field("id", ByteArray.toHexString(getBlockID(block)));

      }
      builder.endObject();

      IndexRequest indexRequest = new IndexRequest("tron", "test")
          .source(builder);
      // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
      esClient.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
    }
  }

  public static void main(String[] args) throws IOException {
    RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(
            new HttpHost("18.223.114.116", 9200, "http")
        ));

    //loadDataFromNode(client);

    GetRequest getRequest = new GetRequest("tron", "test", "AWXS-kRiFwBfzfB0_eBQ");
    try {
      GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
      getResponse.getSource().entrySet().stream().forEach(x -> System.out.println(x.getKey() + " ->" + x.getValue()));
    } catch (ElasticsearchException e) {
      if (e.status() == RestStatus.NOT_FOUND) {
        System.out.println("not find");
      } else {

      }
    }

    SearchRequest searchRequest = new SearchRequest("tron");
    searchRequest.types("test");
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
  //  searchSourceBuilder.sort(new FieldSortBuilder("number").order(SortOrder.DESC));
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
    for (SearchHit documentFields : searchResponse.getHits()) {
      System.out.println(documentFields.field("number").getName() );
    }

    client.close();
  }
}
