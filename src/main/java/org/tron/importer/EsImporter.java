package org.tron.importer;

import com.google.common.primitives.Longs;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.tron.common.crypto.Sha256Hash;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Protocol.Block;
import org.tron.walletserver.WalletApi;


public class EsImporter {

  private static RestHighLevelClient client = new RestHighLevelClient(
      RestClient.builder(
          new HttpHost("18.223.114.116", 9200, "http")
      ));
  private static Properties connectionProperties = new Properties();
  private static Connection dbconnection;

  static {
    try {
      dbconnection = DriverManager.getConnection("jdbc:es://18.223.114.116:9200", connectionProperties);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

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

  public static void deleteByID(String index, String type, String id) throws IOException {
    DeleteRequest request = new DeleteRequest(index, type, id);
    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    request.setRefreshPolicy("wait_for");
    DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
    ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
    if (shardInfo.getFailed() > 0) {
      for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
        String reason = failure.reason();
        System.out.println("delete "+ index + ":" + type + ":" + id + "failed:" + reason);
      }
    }
  }

  public static long getCurrentBlockInDB() throws SQLException {
    Statement statement = dbconnection.createStatement();
    ResultSet results = statement.executeQuery("select max(number) from tron");
    long number = -1;
    while (results.next()) {
      number = results.getLong(1);
    }
    return number;
  }

  public static long getCurrentBlockInSolidity() {
    Block block = WalletApi.getBlock4Loader(-1, false);
    return block.getBlockHeader().getRawData().getNumber();
  }

  public static long getCurrentBlockInFull() {
    Block block = WalletApi.getBlock4Loader(-1, true);
    return block.getBlockHeader().getRawData().getNumber();
  }


  public static void main(String[] args) {

    //  loadDataFromNode(client);

    try {

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        client.close();
        dbconnection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
