package org.tron.importer;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.tron.api.GrpcAPI.BlockList;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.walletserver.WalletApi;


public class EsImporter {

  private ConnectionTool connectionTool = new ConnectionTool();

  public void init() throws IOException {
    CreateIndexRequest createBlockRequest = new CreateIndexRequest("blocks");
    Builder blockSettings = Settings.builder();
    blockSettings.put("index.number_of_shards", 5);
    blockSettings.put("index.max_result_window", 1000000000);
    createBlockRequest.settings(blockSettings);
    GetIndexRequest getIndexRequest = new GetIndexRequest();
    getIndexRequest.indices("blocks");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createBlockRequest, RequestOptions.DEFAULT);
    }


    CreateIndexRequest createTransactionRequest = new CreateIndexRequest("transactions");
    Builder transactionsSettings = Settings.builder();
    transactionsSettings.put("index.number_of_shards", 5);
    transactionsSettings.put("index.max_result_window", 1000000000);
    createTransactionRequest.settings(transactionsSettings);
    getIndexRequest = new GetIndexRequest();
    getIndexRequest.indices("transactions");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createTransactionRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest createTransferRequest = new CreateIndexRequest("transfers");
    Builder transferSettings = Settings.builder();
    transferSettings.put("index.number_of_shards", 5);
    transferSettings.put("index.max_result_window", 1000000000);
    createTransferRequest.settings(transferSettings);
    getIndexRequest = new GetIndexRequest();
    getIndexRequest.indices("transfers");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createTransferRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest createExchangeTransactionRequest = new CreateIndexRequest("exchange_transactions");
    Builder exchangeTransactionSettings = Settings.builder();
    exchangeTransactionSettings.put("index.number_of_shards", 5);
    exchangeTransactionSettings.put("index.max_result_window", 1000000000);
    createExchangeTransactionRequest.settings(exchangeTransactionSettings);
    getIndexRequest = new GetIndexRequest();
    getIndexRequest.indices("exchange_transactions");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices()
          .create(createExchangeTransactionRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest createTriggerRequest = new CreateIndexRequest("smart_contract_triggers");
    Builder triggerSettings = Settings.builder();
    triggerSettings.put("index.number_of_shards", 5);
    triggerSettings.put("index.max_result_window", 1000000000);
    createTriggerRequest.settings(triggerSettings);
    getIndexRequest = new GetIndexRequest();
    getIndexRequest.indices("smart_contract_triggers");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createTriggerRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest createAccountRequest = new CreateIndexRequest("accounts");
    Builder accountSettings = Settings.builder();
    accountSettings.put("index.number_of_shards", 5);
    accountSettings.put("index.max_result_window", 1000000000);
    createAccountRequest.settings(accountSettings);
    getIndexRequest.indices("accounts");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createAccountRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest createInfoRequest = new CreateIndexRequest("transaction_info");
    Builder infoSettings = Settings.builder();
    infoSettings.put("index.number_of_shards", 5);
    infoSettings.put("index.max_result_window", 1000000000);
    createInfoRequest.settings(infoSettings);
    getIndexRequest.indices("transaction_info");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createInfoRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest createParticipateRequest = new CreateIndexRequest("participate_asset_issue");
    Builder participateSettings = Settings.builder();
    participateSettings.put("index.number_of_shards", 5);
    participateSettings.put("index.max_result_window", 1000000000);
    createParticipateRequest.settings(participateSettings);
    getIndexRequest.indices("participate_asset_issue");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createParticipateRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest createVoteRequest = new CreateIndexRequest("vote_witness_contract");
    Builder voteSettings = Settings.builder();
    voteSettings.put("index.number_of_shards", 5);
    voteSettings.put("index.max_result_window", 1000000000);
    createVoteRequest.settings(voteSettings);
    getIndexRequest.indices("vote_witness_contract");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(createVoteRequest, RequestOptions.DEFAULT);
    }

    CreateIndexRequest contractsRequest = new CreateIndexRequest("smart_contracts");
    Builder contractsSettings = Settings.builder();
    contractsSettings.put("index.number_of_shards", 5);
    contractsSettings.put("index.max_result_window", 1000000000);
    contractsRequest.settings(contractsSettings);
    getIndexRequest.indices("smart_contracts");
    if (!connectionTool.client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
      connectionTool.client.indices().create(contractsRequest, RequestOptions.DEFAULT);
    }

  }

  private void parseTransactions(Block block, boolean full) throws IOException {
    for (Transaction transaction : block.getTransactionsList()) {
      Transaction.Contract contract = transaction.getRawData().getContract(0);
      ContractType contractType = contract.getType();
      XContentBuilder builder = XContentFactory.jsonBuilder();
      long createTime = block.getBlockHeader().getRawData().getTimestamp();
      builder.startObject();
      builder.field("date_created", createTime);
      builder.field("block", block.getBlockHeader().getRawData().getNumber());
      builder.field("hash", Util.getTxID(transaction));
      builder.field("contract_data", Util.printTransactionToJSON(transaction));
      builder.field("contract_type", contractType);
      builder.field("owner_address",
          WalletApi.encode58Check(Util.getOwner(contract)));
      builder.field("to_address", Util.getToAddress(contract));
      builder.field("confirmed", !full);
      builder.endObject();
      IndexRequest indexRequest = new IndexRequest("transactions", "transactions",
          Util.getTxID(transaction))
          .source(builder);
      connectionTool.blockBulk.add(indexRequest);

      List<UpdateRequest> updateList = Util.getUpdateBuilder(block, transaction, full);
      if (updateList != null) {
        for (UpdateRequest updateRequest : updateList) {
          connectionTool.blockBulk.add(updateRequest);
        }
      }

      List<IndexRequest> indexList = Util.getIndexBuilder(block, transaction, full);
      if (indexList != null) {
        for (IndexRequest request : indexList) {
          connectionTool.blockBulk.add(request);
        }
      }

//      List<UpdateRequest> addressList = Util.getAddressBuilder(block.getBlockHeader().getRawData().getTimestamp());
//      if (addressList != null) {
//        for (UpdateRequest request : addressList) {
//          connectionTool.blockBulk.add(request);
//        }
//      }
    }
  }

  public void parseBlock(Block block, boolean full) throws IOException {
    System.out.println("parsing block " + block.getBlockHeader().getRawData().getNumber()
        + ", confirmed: " + !full);

    parseTransactions(block, full);

    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    builder.field("hash", Util.getBlockID(block));
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
    builder.endObject();
    IndexRequest indexRequest = new IndexRequest("blocks", "blocks", Util.getBlockID(block))
        .source(builder);
    connectionTool.blockBulk.add(indexRequest);

    if (connectionTool.blockBulk.numberOfActions() >= 10000) {
      connectionTool.bulkSave();
    }
  }

  private void insert(Block block, boolean full) throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    builder.field("hash", "qqqqq");
    builder.field("number", 1500000L);
    builder.field("witness_address", "sss");
    builder.field("parent_hash", "dddd");
    builder.field("date_created", 239479237492L);
    builder.field("trie", "ddd");
    builder.field("witness_id", 11);
    builder.field("transactions", 11);
    builder.field("size", 123);
    builder.field("confirmed", true);
    builder.endObject();
    IndexRequest indexRequest = new IndexRequest("abcd", "abcd", "qqqqq")
        .source(builder);
    connectionTool.client.index(indexRequest, RequestOptions.DEFAULT);
  }

  private void deleteForkedBlock(String hash) throws IOException {
    deleteByID("blocks", "blocks", hash);
    //to do, delete transactions in block
  }

  private List<Block> sortList(BlockList blockList) {
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

  public void loadDataFromNode() throws IOException, SQLException {
    //check if it is a same chain
    checkIsSameChain();

    //check whether the chain forked or not
    long blockInDB = getCurrentBlockNumberInDB();
    String blockHashInDB = getCurrentBlockHashInDB(blockInDB);
    Block checkDBForkedBlock = WalletApi.getBlock4Loader(blockInDB, false);
    while (blockInDB > 0 && checkDBForkedBlock.getSerializedSize() > 0
        && !Util.getBlockID(checkDBForkedBlock).equalsIgnoreCase(blockHashInDB)) {
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
    boolean fullNodeNotForked = Util.getBlockID(checkFullNodeForkedBlock).equalsIgnoreCase(
        Util.getBlockID(blockInSolidity));
    //get solidity blocks in batch mode from full node
    if (fullNodeNotForked) {
      long i = syncBlockFrom;
      while (i <= solidity) {
        if (i + 100 <= solidity) {
          BlockList blockList = WalletApi.getBlockByLimitNext(i, i + 100).get();
          for (Block block : sortList(blockList)) {
            parseBlock(block, false);
          }
          i += 100;
        } else {
          BlockList blockList = WalletApi.getBlockByLimitNext(i, solidity + 1).get();
          for (Block block : sortList(blockList)) {
            parseBlock(block, false);
          }
          i = solidity + 1;
        }
      }
    } else { //sync data from solidity
      for (long i = syncBlockFrom; i <= solidity; i++) {
        Block block = WalletApi.getBlock4Loader(i, false);
        parseBlock(block, false);
      }
    }

    if (connectionTool.blockBulk.numberOfActions() > 0) {
      connectionTool.bulkSave();
    }

    //sync address from solidity
    //syncAddress();

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
    if (connectionTool.blockBulk.numberOfActions() > 0) {
      connectionTool.bulkSave();
    }
  }

  public void deleteByID(String index, String type, String id) throws IOException {
    DeleteRequest request = new DeleteRequest(index, type, id);
    request.setRefreshPolicy("wait_for");
    DeleteResponse deleteResponse = connectionTool.client.delete(request, RequestOptions.DEFAULT);
    ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
    if (shardInfo.getFailed() > 0) {
      for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
        String reason = failure.reason();
        System.out.println("delete " + index + ":" + type + ":" + id + "failed:" + reason);
      }
    }
  }

  public void deleteIndex(String index) throws IOException {
    DeleteIndexRequest request = new DeleteIndexRequest(index);
    request.timeout("2m");
    request.indicesOptions(IndicesOptions.lenientExpandOpen());
    connectionTool.client.indices().delete(request, RequestOptions.DEFAULT);
  }

  private void checkIsSameChain() throws IOException {
    Block block = WalletApi.getBlock4Loader(1, false);
    String hash = "";
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement.executeQuery("select hash from blocks where number=1");
      while (results.next()) {
        hash = results.getString(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (!hash.equals("") && !hash.equalsIgnoreCase(Util.getBlockID(block))) {
      resetDB();
    }
  }

  public void resetDB() throws IOException {
    deleteIndex("blocks");
    deleteIndex("asset_issue_contract");
    deleteIndex("participate_asset_issue");
    deleteIndex("transactions");
    deleteIndex("transfers");
    deleteIndex("witness_create_contract");
    deleteIndex("vote_witness_contract");
    deleteIndex("freeze_balance_contract");
    deleteIndex("accounts");
    deleteIndex("statistics");
    deleteIndex("test");
    deleteIndex("proposals");

    deleteIndex(".security-6 ");
    deleteIndex(".security");
    deleteIndex("abcd");
    deleteIndex("player");
    deleteIndex("qwe");
    deleteIndex("smart_contract_triggers");
    deleteIndex("smart_contracts");
    deleteIndex("statistics");
    deleteIndex("exchange_transactions");
    deleteIndex("exchanges");
    deleteIndex("transaction_info");
  }

  public long getCurrentExchangeID() {
    long number = 0;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select max(id) from exchanges");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return number;
  }

  public boolean containAddress(String address) {
    boolean contain = false;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select * from accounts where address='" + address + "'");
      if (results.next()) {
        contain = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return contain;
  }

  public long getCurrentProposalID() {
    long number = 0;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select max(id) from proposals");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return number;
  }

  public String getProposalApprovedList(long id) {
    String approved = "";
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select approved from proposals where id=" + id);
      while (results.next()) {
        approved = results.getString(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return approved;
  }

  private long getCurrentConfirmedBlockNumberInDB() {
    long number = 0;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select max(number) from blocks where confirmed=true");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return number;
  }

  private long getCurrentBlockNumberInDB() {
    long number = 0;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement.executeQuery("select max(number) from blocks");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return number;
  }

  private String getCurrentBlockHashInDB(long number) {
    String hash = "";
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement.executeQuery("select hash from blocks where number=" + number);
      while (results.next()) {
        hash = results.getString(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return hash;
  }

  public void deleteBlocksFrom(long from) {
    try {
      String hash = "";
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement.executeQuery("select hash, number from blocks where number>=" + from);
      while (results.next()) {
        long number = results.getLong(2);
        hash = results.getString(1);
        System.out.println(number);
        DeleteRequest request = new DeleteRequest("blocks", "blocks", hash);
        connectionTool.blockBulk.add(request);
        if(connectionTool.blockBulk.numberOfActions() >= 10000) {
          connectionTool.bulkSave();
        }
      }
      connectionTool.bulkSave();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Block getCurrentBlockInSolidity() {
    Block block = WalletApi.getBlock4Loader(-1, false);
    return block;
  }

  private Block getCurrentBlockInFull() {
    Block block = WalletApi.getBlock4Loader(-1, true);
    return block;
  }

  public static void main(String[] args) {
    try {
      EsImporter importer = new EsImporter();
      importer.init();

      Statistic statistic = new Statistic();
      UpdateAccount updateAccount = new UpdateAccount();
      TotalStatistics totalStatistics = new TotalStatistics();
      LoadTransactionInfo loadTransactionInfo = new LoadTransactionInfo();

      ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println("sync data from block chain at:" + new Date());
          importer.loadDataFromNode();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, 0, 2, TimeUnit.SECONDS);

      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println("sync address from block chain at:" + new Date());
          Util.syncAddress();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, 0, 15, TimeUnit.SECONDS);

      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println("statistic at:" + new Date());
          statistic.statistic();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, WalletApi.hours * 60 * 60, 30, TimeUnit.SECONDS);

      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println("update accounts at:" + new Date());
          updateAccount.UpdateAccounts();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, WalletApi.hours * 60 * 60, 2, TimeUnit.SECONDS);

      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println("totalStatistics at:" + new Date());
          totalStatistics.statistics();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, WalletApi.hours * 60 * 60, 5, TimeUnit.SECONDS);

      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println("load transactionInfo at:" + new Date());
          loadTransactionInfo.LoadData();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, WalletApi.hours * 60 * 60, 5, TimeUnit.SECONDS);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}