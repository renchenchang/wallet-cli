package org.tron.importer;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class Statistic {

  private long startTime = 1529856000000L;
  private ConnectionTool connectionTool = new ConnectionTool();

  private long getNextStatisticTime() {
    long time = startTime;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select max(statistic_time) from statistics");
      if (results.next()) {
        time = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return time ;
  }

  private long statisticAccounts(long time) {
    long count = 0;
    long startTime = time;
    long endTime = time + 5 * 60 * 1000;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select count(*) from accounts where date_created>="
              + startTime + " and date_created<" + endTime);
      if (results.next()) {
        count = results.getLong(1);
      }
    } catch (Exception e) {

    }
    return count;
  }

  private long statisticTransactions(long time) {
    long count = 0;
    long startTime = time;
    long endTime = time + 5 * 60 * 1000;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select count(*) from transactions where date_created>="
              + startTime + " and date_created<" + endTime);
      if (results.next()) {
        count = results.getLong(1);
      }
    } catch (Exception e) {

    }
    return count;
  }

  private long statisticBlockSize(long time) {
    long count = 0;
    long startTime = time;
    long endTime = time + 5 * 60 * 1000;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select sum(size) from blocks where date_created>="
              + startTime + " and date_created<" + endTime);
      if (results.next()) {
        count = results.getLong(1);
      }
    } catch (Exception e) {

    }
    return count;
  }

  private void statistic() throws IOException {
    long time = getNextStatisticTime();
    while(time + 5 * 60 * 1000 < System.currentTimeMillis()) {
      System.out.println("current time is " + time);
      long accountNum = statisticAccounts(time);
      long transactionNum = statisticTransactions(time);
      long blockSize = statisticBlockSize(time);
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      builder.field("statistic_time", time);
      builder.field("accounts", accountNum);
      builder.field("transactions", transactionNum);
      builder.field("block_size", blockSize);
      builder.endObject();
      IndexRequest indexRequest = new IndexRequest("statistics", "statistics", time + "")
          .source(builder);
      connectionTool.client.index(indexRequest, RequestOptions.DEFAULT);
      time = getNextStatisticTime() + 5 * 60 * 1000;
    }
  }


  public static void main(String[] args) {
    Statistic statistic = new Statistic();
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
      scheduledExecutorService.scheduleAtFixedRate(() -> {
        try {
          System.out.println("statistic at " + new Date());
          statistic.statistic();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }, 0, 5, TimeUnit.MINUTES);
  }
}
