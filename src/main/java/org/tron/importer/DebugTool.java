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
      long max = 0;
      String user = "";
      String contract = "";
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select  owner_address from smart_contract_triggers group by owner_address");
      while (results.next()) {
        String owner_address = results.getString(1);
//        String contract_address = results.getString(2);
//        long count = results.getLong(3);
        max++;
      }

      System.out.println(user + "\t" + contract + "\t" + max);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    DebugTool debugTool = new DebugTool();
    debugTool.LoadData();
  }
}
