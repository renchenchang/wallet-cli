package org.tron.importer;


import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Protocol.Block;
import org.tron.walletserver.WalletApi;


public class EsImporter {





  public static void main(String[] args) throws IOException {

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

    RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(
            new HttpHost("18.223.114.116", 9200, "http")
         ));
    for (int i=0; i< 5000; i++) {
      Block block = WalletApi.getBlock(i);
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      {
        builder.field("number", block.getBlockHeader().getRawData().getNumber());
        builder.field("witness", WalletApi
            .encode58Check(block.getBlockHeader().getRawData().getWitnessAddress().toByteArray()));
        builder.field("parent",
            ByteArray.toHexString(block.getBlockHeader().getRawData().getParentHash().toByteArray()));

      }
      builder.endObject();
      IndexRequest indexRequest = new IndexRequest("tron", "blocks")
          .source(builder);
     // IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

      client.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
    }
    client.close();
  }
}
