package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import java.sql.ResultSet;
import java.sql.Statement;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class DebugTool {
  private ConnectionTool connectionTool = new ConnectionTool();

  public void LoadData() {
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select hash from smart_contract_triggers ");
      while (results.next()) {
        String hash = results.getString(1);
        UpdateRequest updateRequest = new UpdateRequest("smart_contract_triggers", "smart_contract_triggers", hash);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("need_result", 1);
        updateRequest.doc(jsonObject.toJSONString(), XContentType.JSON);
        connectionTool.blockBulk.add(updateRequest);
        if (connectionTool.blockBulk.numberOfActions() >= 10000) {
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

  public void updateExchange(long[] ids) {
    try {
      for (long id : ids) {
        UpdateRequest updateRequest = new UpdateRequest("exchanges", "exchanges", id + "");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("checked", 1);
        updateRequest.doc(jsonObject.toJSONString(), XContentType.JSON);
        connectionTool.blockBulk.add(updateRequest);
      }
      if (connectionTool.blockBulk.numberOfActions() > 0) {
        connectionTool.bulkSave();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    DebugTool debugTool = new DebugTool();
    debugTool.updateExchange(new long[]{2, 7, 9});
  }
}
