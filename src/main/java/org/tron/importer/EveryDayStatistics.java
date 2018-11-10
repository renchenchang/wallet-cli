package org.tron.importer;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class EveryDayStatistics {

  private ConnectionTool connectionTool = new ConnectionTool();

  public void exchangeStartPrice() {
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select id, first_token_id, first_token_balance, second_token_id, second_token_balance from exchanges where confirmed=true");
      while (results.next()) {
        long id = results.getLong(1);
        String firstToken = results.getString(2);
        long firstBalance = results.getLong(3);
        String secondToken = results.getString(4);
        long secondBalance = results.getLong(5);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("id", id);
        builder.field("first_token_id", firstToken);
        builder.field("first_token_balance", firstBalance);
        builder.field("second_token_id", secondToken);
        builder.field("second_token_balance", secondBalance);
        builder.field("date_created", System.currentTimeMillis());
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("exchange_start_price", "exchange_start_price", id + "")
            .source(builder);
        connectionTool.blockBulk.add(indexRequest);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public void statistics() throws IOException {
    exchangeStartPrice();
    connectionTool.bulkSave();
  }
}
