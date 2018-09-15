package org.tron.importer;

import com.google.common.primitives.Longs;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.tron.common.crypto.Sha256Hash;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Protocol.Block;
import org.tron.walletserver.WalletApi;


public class EsImporter {

  private static RestHighLevelClient client;
  private static Properties connectionProperties = new Properties();
  private static Connection dbConnection;
  private static BulkRequest blockBulk = new BulkRequest();

  static {
    try {
      client = new RestHighLevelClient(
          RestClient.builder(
              new HttpHost("18.223.114.116", 9200, "http")
          ));
      blockBulk.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
      blockBulk.setRefreshPolicy("wait_for");
      blockBulk.timeout(TimeValue.timeValueMinutes(2));
      blockBulk.timeout("2m");
      dbConnection = DriverManager
          .getConnection("jdbc:es://18.223.114.116:9200", connectionProperties);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static Connection getConn() throws SQLException {
    if (dbConnection.isClosed()) {
      dbConnection = DriverManager
          .getConnection("jdbc:es://18.223.114.116:9200", connectionProperties);
    }
    return dbConnection;
  }

  private static String getBlockID(Block block) {
    long blockNum = block.getBlockHeader().getRawData().getNumber();
    byte[] blockHash = Sha256Hash.of(block.getBlockHeader().getRawData().toByteArray())
        .getByteString().toByteArray();
    byte[] numBytes = Longs.toByteArray(blockNum);
    byte[] hash = new byte[blockHash.length];
    System.arraycopy(numBytes, 0, hash, 0, 8);
    System.arraycopy(blockHash, 8, hash, 8, blockHash.length - 8);
    return ByteArray.toHexString(hash);
  }

  private static void bulkSave() throws IOException {
    client.bulk(blockBulk, RequestOptions.DEFAULT);
    blockBulk.requests().clear();
    //to do, save transaction in block
  }

  private static void parseBlock(Block block, boolean full) throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    {
      builder.field("hash", getBlockID(block));
      builder.field("number", block.getBlockHeader().getRawData().getNumber());
      builder.field("witness_address", WalletApi
          .encode58Check(block.getBlockHeader().getRawData().getWitnessAddress().toByteArray()));
      builder.field("parent_hash",
          ByteArray
              .toHexString(block.getBlockHeader().getRawData().getParentHash().toByteArray()));
      builder.field("date_created", block.getBlockHeader().getRawData().getTimestamp());
      builder.field("trie",
          WalletApi
              .encode58Check(block.getBlockHeader().getRawData().getTxTrieRoot().toByteArray()));
      builder.field("witness_id", block.getBlockHeader().getRawData().getWitnessId());
      builder.field("transactions", block.getTransactionsCount());
      builder.field("size", block.toByteArray().length);
      builder.field("confirmed", !full);
    }
    builder.endObject();

    IndexRequest indexRequest = new IndexRequest("blocks", "blocks", getBlockID(block))
        .source(builder);
    blockBulk.add(indexRequest);
    if (blockBulk.numberOfActions() >= 5000) {
      bulkSave();
    }
  }

  private static void deleteForkedBlock(String hash, long number) throws IOException {
    deleteByID("blocks", "blocks", hash);
    //to do, delete transactions in block
  }

  public static void loadDataFromNode() throws IOException, SQLException {
    //check if it is a same chain
    checkIsSameChain();

    long blockInDB = getCurrentBlockNumberInDB();
    String blockHashInDB = getCurrentBlockHashInDB(blockInDB);

    //check whether the chain forked or not
    Block checkForkedDbBlock = WalletApi.getBlock4Loader(blockInDB, false);
    while (blockInDB > 0 && checkForkedDbBlock.getSerializedSize() > 0
        && !getBlockID(checkForkedDbBlock).equalsIgnoreCase(blockHashInDB)) {
      //if forked, delete forked blocks in db
      deleteForkedBlock(blockHashInDB, blockInDB);
      blockInDB = getCurrentBlockNumberInDB();
      blockHashInDB = getCurrentBlockHashInDB(blockInDB);
    }

    //sync data from solidity
    Block blockInFullNode = getCurrentBlockInFull();
    Block blockInSolidity = getCurrentBlockInSolidity();
    long fullNode = blockInFullNode.getBlockHeader().getRawData().getNumber();
    long solidity = blockInSolidity.getBlockHeader().getRawData().getNumber();
    for (long i = blockInDB; i <= solidity; i++) {
      Block block = WalletApi.getBlock4Loader(i, false);
      parseBlock(block, false);
    }

    //sync data from fullnode
    if (fullNode > solidity) {
      Block checkForkBlock = WalletApi.getBlock4Loader(solidity, true);
      if (getBlockID(checkForkBlock).equalsIgnoreCase(getBlockID(blockInSolidity))) {
        for (long j = solidity + 1; j <= fullNode; j++) {
          Block block = WalletApi.getBlock4Loader(j, true);
          parseBlock(block, true);
        }
      }
    }

    if (blockBulk.numberOfActions() > 0) {
      bulkSave();
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
        System.out.println("delete " + index + ":" + type + ":" + id + "failed:" + reason);
      }
    }
  }

  private static void deleteIndex(String index) throws IOException {
    DeleteIndexRequest request = new DeleteIndexRequest(index);
    request.timeout(TimeValue.timeValueMinutes(2));
    request.timeout("2m");
    request.indicesOptions(IndicesOptions.lenientExpandOpen());
    client.indices().delete(request, RequestOptions.DEFAULT);
  }

  private static void checkIsSameChain() throws IOException {
    Block block = WalletApi.getBlock4Loader(1, false);
    String hash = "";
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select hash from blocks where number=1");
      while (results.next()) {
        hash = results.getString(1);
      }
    } catch (Exception e) {
    }
    if (!hash.equals("") && !hash.equalsIgnoreCase(getBlockID(block))) {
      resetDB();
    }
  }

  private static void resetDB() throws IOException {
    deleteIndex("blocks");
  }

  private static long getCurrentBlockNumberInDB() {
    long number = 0;
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select max(number) from blocks");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {

    }
    return number;
  }

  private static String getCurrentBlockHashInDB(long number) {
    String hash = "";
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select hash from blocks where number=" + number);
      while (results.next()) {
        hash = results.getString(1);
      }
    } catch (Exception e) {

    }
    return hash;
  }

  private static Block getCurrentBlockInSolidity() {
    Block block = WalletApi.getBlock4Loader(-1, false);
    return block;
  }

  private static Block getCurrentBlockInFull() {
    Block block = WalletApi.getBlock4Loader(-1, true);
    return block;
  }

  public static void main(String[] args) {
    try {
      ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
          loadDataFromNode();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, 2, 2, TimeUnit.SECONDS);
      // resetDB();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        // client.close();
        //  dbConnection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
