package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.TransactionInfo;
import org.tron.walletserver.WalletApi;

public class LoadTransactionInfo {

  private ConnectionTool connectionTool = new ConnectionTool();

  public void LoadData() {
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select hash from smart_contract_triggers where ((need_result is null) or need_result=1) and confirmed=true");
      while (results.next()) {
        String txid = results.getString(1);
        Optional<TransactionInfo> transactionInfo = WalletApi.getTransactionInfoById(txid);
        Optional<Transaction> transaction = WalletApi.getTransactionById(txid);
        if (transactionInfo.isPresent()) {
          TransactionInfo info = transactionInfo.get();
          String triggerContractAddress = WalletApi.encode58Check(info.getContractAddress().toByteArray());
          String ownerAddress = WalletApi.encode58Check(Util.getOwner(transaction.get().getRawData().getContract(0)));
          long blockNum = info.getBlockNumber();
          long time = info.getBlockTimeStamp();
          long energyFee =  info.getReceipt().getEnergyFee();
          long netFee =  info.getReceipt().getNetFee();
          long energyUsage = info.getReceipt().getEnergyUsage();
          long netUsage = info.getReceipt().getNetUsage();
          long originEnergyUsage = info.getReceipt().getOriginEnergyUsage();
          long energyUsageTotal = info.getReceipt().getEnergyUsageTotal();
          String exeResult = info.getReceipt().getResult().getValueDescriptor().getName();
          XContentBuilder transactionInfoBuilder = XContentFactory.jsonBuilder();
          transactionInfoBuilder.startObject();
          transactionInfoBuilder.field("hash", txid);
          transactionInfoBuilder.field("owner_address", ownerAddress);
          transactionInfoBuilder.field("block", blockNum);
          transactionInfoBuilder.field("date_created", time);
          transactionInfoBuilder.field("contract_address", triggerContractAddress);
          transactionInfoBuilder.field("energy_fee", energyFee);
          transactionInfoBuilder.field("net_fee", netFee);
          transactionInfoBuilder.field("energy_usage", energyUsage);
          transactionInfoBuilder.field("net_usage", netUsage);
          transactionInfoBuilder.field("origin_energy_usage", originEnergyUsage);
          transactionInfoBuilder.field("energy_usage_total", energyUsageTotal);
          transactionInfoBuilder.field("exe_result", exeResult);
          transactionInfoBuilder.field("confirmed", true);
          transactionInfoBuilder.field("day", Util.getCurrentEpochTimeStamp(time));
          transactionInfoBuilder.endObject();
          IndexRequest indexRequest = new IndexRequest("transaction_info", "transaction_info", txid)
              .source(transactionInfoBuilder);
          connectionTool.blockBulk.add(indexRequest);

          UpdateRequest updateRequest = new UpdateRequest("smart_contract_triggers",
              "smart_contract_triggers", txid);
          JSONObject jsonObject = new JSONObject();
          jsonObject.put("need_result", 0);
          updateRequest.doc(jsonObject.toJSONString(), XContentType.JSON);
          connectionTool.blockBulk.add(updateRequest);
//        for (Log log : info.getLogList()) {
//
//        }
          if (connectionTool.blockBulk.numberOfActions() >= 10000) {
            connectionTool.bulkSave();
          }
        }
      }
      if (connectionTool.blockBulk.numberOfActions() > 0) {
        connectionTool.bulkSave();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    LoadTransactionInfo load = new LoadTransactionInfo();
    load.LoadData();
  }
}
