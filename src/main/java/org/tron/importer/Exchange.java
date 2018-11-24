package org.tron.importer;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class Exchange {

  private ConnectionTool connectionTool = new ConnectionTool();



//  public void exchanges() {
//    try {
//      Optional<ExchangeList> exchanges = WalletApi.listExchanges();
//      if (exchanges.isPresent()) {
//        for (Exchange exchange : exchanges.get().getExchangesList()) {
//
//          long id = exchange.getExchangeId();
//          String firstToken = exchange.getFirstTokenId().toStringUtf8();
//          long firstBalance = exchange.getFirstTokenBalance();
//          String secondToken = exchange.getSecondTokenId().toStringUtf8();
//          long secondBalance = exchange.getSecondTokenBalance();
//          XContentBuilder builder = XContentFactory.jsonBuilder();
//          builder.startObject();
//          builder.field("owner_address", "owner_address");
//          builder.field("block", 0);
//          builder.field("hash", "hash");
//          builder.field("date_created", 0);
//          builder
//              .field("first_token_id", firstToken);
//          builder.field("first_token_balance", firstBalance);
//          builder
//              .field("second_token_id", secondToken);
//          builder.field("second_token_balance", secondBalance);
//          builder.field("id", id);
//          builder.field("confirmed", true);
//          builder.endObject();
//          IndexRequest indexRequest = new IndexRequest("exchanges", "exchanges", id + "")
//              .source(builder);
//
//
//          connectionTool.blockBulk.add(indexRequest);
//        }
//      }
//      connectionTool.bulkSave();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }

  public void exchangePrice() {
    try {
      Statement statement = connectionTool.getConn().createStatement();
      statement.setFetchSize(10000);
      ResultSet results = statement
          .executeQuery("select id, first_token_id, first_token_balance, second_token_id, second_token_balance "
              + "from exchanges where confirmed=true");
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
        IndexRequest indexRequest = new IndexRequest("exchange_prices", "exchange_prices", UUID.randomUUID().toString())
            .source(builder);
        connectionTool.blockBulk.add(indexRequest);
        if (connectionTool.blockBulk.numberOfActions() >= 500) {
          connectionTool.bulkSave();
        }
      }
      connectionTool.bulkSave();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Exchange exchange = new Exchange();
    exchange.exchangePrice();
  }
}
