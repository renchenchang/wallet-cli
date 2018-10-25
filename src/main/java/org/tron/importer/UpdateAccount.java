package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import java.sql.ResultSet;
import java.sql.Statement;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
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

  public static void main(String[] args) {
    UpdateAccount updateAccount = new UpdateAccount();
    updateAccount.UpdateAccounts();
  }
}

