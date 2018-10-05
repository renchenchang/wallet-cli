package org.tron.importer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ConnectionTool {
  public RestHighLevelClient client;
  public Properties connectionProperties = new Properties();
  public Connection dbConnection;
  public BulkRequest blockBulk = new BulkRequest();

  public ConnectionTool() {
    try {
      client = new RestHighLevelClient(
          RestClient.builder(
              new HttpHost("18.223.114.116", 9200, "http")
          ));
      blockBulk.setRefreshPolicy("wait_for");
      blockBulk.timeout("2m");
      dbConnection = DriverManager
          .getConnection("jdbc:es://18.223.114.116:9200", connectionProperties);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public Connection getConn() throws SQLException {
    if (dbConnection.isClosed()) {
      dbConnection = DriverManager
          .getConnection("jdbc:es://18.223.114.116:9200", connectionProperties);
    }
    return dbConnection;
  }

  public void bulkSave() throws IOException {
    client.bulk(blockBulk, RequestOptions.DEFAULT);
    blockBulk.requests().clear();
  }
}
