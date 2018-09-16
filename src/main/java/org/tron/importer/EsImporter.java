package org.tron.importer;

import com.google.common.primitives.Longs;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.tron.api.GrpcAPI.BlockList;
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
    System.out.println("parsing block " + block.getBlockHeader().getRawData().getNumber()
    + ", confirmed: " + !full);
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

  private static void deleteForkedBlock(String hash) throws IOException {
    deleteByID("blocks", "blocks", hash);
    //to do, delete transactions in block
  }

  private static List<Block> sortList(BlockList blockList) {
    ArrayList<Block> list = new ArrayList<>();
    list.addAll(blockList.getBlockList());
    list.sort((x, y) -> {
          long num1 = x.getBlockHeader().getRawData().getNumber();
          long num2 = y.getBlockHeader().getRawData().getNumber();
          if (num1 > num2) {
            return 1;
          } else if (num1 == num2) {
            return 0;
          } else {
            return -1;
          }
        }
    );
    return list;
  }


  public static void loadDataFromNode() throws IOException, SQLException {
    //check if it is a same chain
    checkIsSameChain();

    //check whether the chain forked or not
    long blockInDB = getCurrentBlockNumberInDB();
    String blockHashInDB = getCurrentBlockHashInDB(blockInDB);
    Block checkDBForkedBlock = WalletApi.getBlock4Loader(blockInDB, false);
    while (blockInDB > 0 && checkDBForkedBlock.getSerializedSize() > 0
        && !getBlockID(checkDBForkedBlock).equalsIgnoreCase(blockHashInDB)) {
      //if forked, delete forked blocks in db
      deleteForkedBlock(blockHashInDB);
      blockInDB = getCurrentBlockNumberInDB();
      blockHashInDB = getCurrentBlockHashInDB(blockInDB);
    }

    //sync data from solidity
    long syncBlockFrom = getCurrentConfirmedBlockNumberInDB() + 1;
    Block blockInSolidity = getCurrentBlockInSolidity();
    long solidity = blockInSolidity.getBlockHeader().getRawData().getNumber();
    Block checkFullNodeForkedBlock = WalletApi.getBlock4Loader(solidity, true);
    boolean fullNodeNotForked = getBlockID(checkFullNodeForkedBlock).equalsIgnoreCase(getBlockID(blockInSolidity));
    //get solidity blocks in batch mode from full node
    if(fullNodeNotForked) {
      long i = syncBlockFrom;
      while (i<=solidity) {
        if (i+100 <= solidity) {
          BlockList blockList = WalletApi.getBlockByLimitNext(i, i+100).get();
          for (Block block : sortList(blockList)) {
            parseBlock(block, false);
          }
          i += 100;
        } else {
          if(solidity > i) {
            BlockList blockList = WalletApi.getBlockByLimitNext(i, solidity + 1).get();
            for (Block block : sortList(blockList)) {
              parseBlock(block, false);
            }
            i = solidity + 1;
          }
        }
      }
    } else { //sync data from solidity
      for (long i=syncBlockFrom; i<=solidity; i++) {
        Block block = WalletApi.getBlock4Loader(i, false);
        parseBlock(block, false);
      }
    }
    if (blockBulk.numberOfActions() > 0) {
      bulkSave();
    }

    //sync data from full node
    long fullNode = getCurrentBlockInFull().getBlockHeader().getRawData().getNumber();
    long currentBlockInDB = getCurrentBlockNumberInDB();
    if (fullNode > solidity) {
      if (fullNodeNotForked) {
        for (long j = currentBlockInDB + 1; j <= fullNode; j++) {
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
    request.timeout("10s");
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

  private static long getCurrentConfirmedBlockNumberInDB() {
    long number = 0;
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select max(number) from blocks where confirmed=true");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {

    }
    return number;
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
          System.out.println("sync data from block chain at:" + new Date());
          loadDataFromNode();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, 2, 2, TimeUnit.SECONDS);
    //  resetDB();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
//         client.close();
//         dbConnection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
