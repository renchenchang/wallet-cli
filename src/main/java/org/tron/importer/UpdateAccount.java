package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.tron.protos.Protocol.Account;
import org.tron.walletserver.WalletApi;

public class UpdateAccount {

  private ConnectionTool connectionTool = new ConnectionTool();

  public void UpdateAccounts() {
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select address from accounts where need_update=1");
      while (results.next()) {
        String address = results.getString(1);
        System.out.println("syncing address: " + address);
        Account account = WalletApi.queryAccount(WalletApi.decodeFromBase58Check(address));
        UpdateRequest updateRequest = new UpdateRequest("accounts", "accounts", address);
        JSONObject jsonObject = JSONObject.parseObject(JsonFormat.printToString(account));
        jsonObject.put("need_update", 0);
        updateRequest.doc(jsonObject.toJSONString(), XContentType.JSON);
        connectionTool.blockBulk.add(updateRequest);
        if (connectionTool.blockBulk.numberOfActions() >= 500) {
          connectionTool.bulkSave();
        }
      }
      if (connectionTool.blockBulk.numberOfActions() > 0) {
        connectionTool.bulkSave();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void saveExchanges() throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();

    builder.field("owner_address", "TYMmLJeReBpsGT1nA51LkjJf3ipP9JD2ej");
    builder.field("block", 3242162);
    builder.field("hash", "faf0df38457d11d426ceea966792a33b2795cf78849dd1481f0ded02d891d87a");
    builder.field("date_created", 1539673401000l);
    builder.field("first_token_id", "LuckyChipsCoin");
    builder.field("first_token_balance", 1000000);
    builder.field("second_token_id", "_");
    builder.field("second_token_balance", 100000000);
    builder.field("id",  1);
    builder.field("confirmed", true);
    builder.endObject();
    IndexRequest indexRequest = new IndexRequest("exchanges", "exchanges", "1")
        .source(builder);
    connectionTool.blockBulk.add(indexRequest);
    connectionTool.bulkSave();
  }

  public static void main(String[] args) throws IOException {
    UpdateAccount updateAccount = new UpdateAccount();
    updateAccount.saveExchanges();
  }
}

