package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.tron.api.GrpcAPI.WitnessList;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Protocol.Witness;
import org.tron.walletserver.WalletApi;

public class TotalStatistics {

  private ConnectionTool connectionTool = new ConnectionTool();

  public void tokenIssued() {
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select to_address,token_name,sum(amount) from"
              + " participate_asset_issue group by to_address, token_name");
      while (results.next()) {
        String issuer = results.getString(1);
        long totalIssued = results.getLong(3);

        UpdateRequest updateRequest = new UpdateRequest("asset_issue_contract",
            "asset_issue_contract", issuer);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("total_issued", totalIssued);
        updateRequest.doc(jsonObject.toJSONString(), XContentType.JSON);
        connectionTool.blockBulk.add(updateRequest);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void witnessVotes() {
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select votes.vote_address, sum(votes.vote_count) from accounts group by votes.vote_address");
      while (results.next()) {
        String witness = results.getString(1);
        long votes = results.getLong(2);

        UpdateRequest updateRequest = new UpdateRequest("witness_create_contract",
            "witness_create_contract", witness);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("votes_realtime", votes);
        updateRequest.doc(jsonObject.toJSONString(), XContentType.JSON);
        connectionTool.blockBulk.add(updateRequest);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String getAccountName(String address) {
    String name = "";
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select account_name from  accounts where address='" + address + "'");
      while (results.next()) {
        name = results.getString(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return name;
  }

  public void witnessWork() {
    try {
      Optional<WitnessList> WitnessList =  WalletApi.listWitnesses();
      if(WitnessList.isPresent()) {
        for (Witness witness : WitnessList.get().getWitnessesList()) {
          String address = WalletApi.encode58Check(witness.getAddress().toByteArray());
          boolean isWorking = witness.getIsJobs();
          long votesOfPreMaintenance = witness.getVoteCount();
          String name = getAccountName(address);
          name = ByteString.copyFrom(ByteArray.fromHexString(name)).toStringUtf8();
          UpdateRequest updateRequest = new UpdateRequest("witness_create_contract",
              "witness_create_contract", address);
          JSONObject jsonObject = new JSONObject();
          jsonObject.put("is_working", isWorking);
          jsonObject.put("votes_pre_maintenance", votesOfPreMaintenance);
          jsonObject.put("account_name", name);
          updateRequest.doc(jsonObject.toJSONString(), XContentType.JSON);
          connectionTool.blockBulk.add(updateRequest);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public void statistics() throws IOException {
    tokenIssued();
    witnessVotes();
    witnessWork();
    connectionTool.bulkSave();
  }

  public static void main(String[] args) throws IOException {
    TotalStatistics totalStatistics = new TotalStatistics();
    totalStatistics.statistics();
  }
}
