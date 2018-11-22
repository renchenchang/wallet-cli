package org.tron.importer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.tron.protos.Protocol.Block;
import org.tron.walletserver.WalletApi;

public class ConnectionTool {
  public RestHighLevelClient client;
  public Properties connectionProperties = new Properties();
  public Connection dbConnection;
  public BulkRequest blockBulk = new BulkRequest();

  public ConnectionTool() {
    try {
      client = new RestHighLevelClient(
          RestClient.builder(
              new HttpHost(WalletApi.es, WalletApi.esPort, "http")
          ).setMaxRetryTimeoutMillis(90000000)
      );
      blockBulk.setRefreshPolicy("wait_for");
      blockBulk.timeout("2m");
      dbConnection = DriverManager
          .getConnection("jdbc:es://"+WalletApi.es+":" + WalletApi.esPort, connectionProperties);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public Connection getConn() throws SQLException {
    if (dbConnection.isClosed()) {
      dbConnection = DriverManager
          .getConnection("jdbc:es://" + WalletApi.es + ":" + WalletApi.esPort, connectionProperties);
    }
    return dbConnection;
  }

  public void bulkSave() throws IOException {
    System.out.println("save to es at:" + new Date());
    System.out.println("before save,the number of actions is " + blockBulk.numberOfActions());
    client.bulk(blockBulk, RequestOptions.DEFAULT);
    blockBulk.requests().clear();
  //  System.out.println("after save,the number of actions is " + blockBulk.numberOfActions());
  }

  public HashMap<String, JSONArray> getExchangeHistory(long start) {
    HashMap<String, JSONArray> priceHistory = new HashMap<>();
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select id, date_created, first_token_id, first_token_balance, second_token_id, second_token_balance "
              + "from exchange_prices where date_created>" + start + " order by date_created ASC");
      while (results.next()) {
        long id = results.getLong(1);
        long time = results.getLong(2);
        String firstToken = results.getString(3);
        long firstBalance = results.getLong(4);
        String secondToken = results.getString(5);
        long secondBalance = results.getLong(6);
        JSONArray list = new JSONArray();
        if (priceHistory.containsKey(Long.toString(id))) {
          list = priceHistory.get(Long.toString(id));
        }
        JSONObject json = new JSONObject();
        json.put("first_token_id", firstToken);
        json.put("first_token_balance", firstBalance);
        json.put("second_token_id", secondToken);
        json.put("second_token_balance", secondBalance);
        json.put("date_created", time);
        list.add(json);
        priceHistory.put(Long.toString(id), list);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return priceHistory;
  }

  public long getCurrentExchangeID() {
    long number = 0;
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select max(id) from exchanges");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {
      number = 0;
      e.printStackTrace();
    }
    return number;
  }

  public boolean containAddress(String address) {
    boolean contain = false;
    try {
      Statement statement = getConn().createStatement();
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
      Statement statement = getConn().createStatement();
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
      Statement statement = getConn().createStatement();
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

  public long getCurrentConfirmedBlockNumberInDB() {
    long number = 0;
    try {
      Statement statement = getConn().createStatement();
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

  public long getCurrentBlockNumberInDB() {
    long number = 0;
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select max(number) from blocks");
      while (results.next()) {
        number = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return number;
  }

  public String getCurrentBlockHashInDB(long number) {
    String hash = "";
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select hash from blocks where number=" + number);
      while (results.next()) {
        hash = results.getString(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return hash;
  }

  public boolean existExchangeStartPrice(long id) {
    boolean exist = false;
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select id from exchange_start_price where id=" + id);
      while (results.next()) {
        exist = true;
      }
    } catch (Exception e) {
      exist = false;
      e.printStackTrace();
    }
    return exist;
  }

}
