package org.tron.importer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.tron.walletserver.WalletApi;

public class CoinDay {
  private static Properties connectionProperties = new Properties();
  private static Connection dbConnection;

  private static Connection getConn() throws SQLException {
    if (dbConnection==null || dbConnection.isClosed()) {
      dbConnection = DriverManager
          .getConnection("jdbc:es://"+ WalletApi.es + ":" + WalletApi.esPort, connectionProperties);
    }
    return dbConnection;
  }

  public static void main(String[] args) {
    try {
      Statement statement = getConn().createStatement();
      ResultSet results = statement.executeQuery("select * from freeze_balance_contract order by owner_address, block desc");

      float coinDay = 0;
      String lastAddress = "";
      long now = System.currentTimeMillis();
      boolean startAdd = true;
      while (results.next()) {
        String address = results.getString(6);
        String type = results.getString(7);
        long amount = results.getLong(3);
        long time = results.getLong(2);
        if (address.equals(lastAddress)) {

        } else {
          if(!lastAddress.equals("") && startAdd) {
            System.out.println(lastAddress + "\t" + coinDay);
          }
          lastAddress = address;
          coinDay = 0;
          startAdd = true;
        }
        if (type.equals("FreezeBalanceContract") && startAdd) {
          coinDay += amount * 1.0 * (now - time) / (24 * 60 * 60 *1000) / 1000000;
        } else {
          if(startAdd) {
            System.out.println(lastAddress + "\t" + coinDay);
          }
          startAdd= false;
          coinDay = 0;
        }

      }

      System.out.println(lastAddress + "\t" + coinDay);


    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
}
