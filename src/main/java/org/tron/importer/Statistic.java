package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

public class Statistic {

  private long startTime = 1529856000000L;
  private ConnectionTool connectionTool = new ConnectionTool();
  private long period = 24 * 60 * 60 * 1000;

  private long getMaxStatisticTime() {
    long time = startTime;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select max(statistic_time) from statistics where end=TRUE");
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
    long endTime = time + period;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select count(*) from accounts where date_created>="
              + startTime + " and date_created<" + endTime);
      if (results.next()) {
        count = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  private long statisticTransactions(long time) {
    long count = 0;
    long startTime = time;
    long endTime = time + period;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select count(*) from transactions where date_created>="
              + startTime + " and date_created<" + endTime);
      if (results.next()) {
        count = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  private long getMaxBlockTimeInES() {
    long time = startTime;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select max(date_created) from blocks");
      if (results.next()) {
        time = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return time;
  }

  private long statisticBlockSize(long time) {
    long count = 0;
    long startTime = time;
    long endTime = time + period;
    try {
      Statement statement = connectionTool.getConn().createStatement();
      ResultSet results = statement
          .executeQuery("select sum(size) from blocks where date_created>="
              + startTime + " and date_created<" + endTime);
      if (results.next()) {
        count = results.getLong(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  public void statistic() throws IOException {
    long time = getMaxStatisticTime();
    long maxBlockTime = getMaxBlockTimeInES();
    SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
    while(time  <= maxBlockTime) {
      String day = sdf.format(new Date(time));
      System.out.println("current day is " + day);
      long accountNum = statisticAccounts(time);
      long transactionNum = statisticTransactions(time);
      long blockSize = statisticBlockSize(time);
      boolean end = (time + period) <= maxBlockTime ? true : false;
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      builder.field("statistic_time", time);
      builder.field("accounts", accountNum);
      builder.field("transactions", transactionNum);
      builder.field("block_size", blockSize);
      builder.field("day", day);
      builder.field("end", end);
      builder.endObject();
      IndexRequest indexRequest = new IndexRequest("statistics", "statistics", day)
          .source(builder);
      UpdateRequest updateRequest = new UpdateRequest("statistics", "statistics",
          day);
      JSONObject updateStatistic = new JSONObject();
      updateStatistic.put("accounts", accountNum);
      updateStatistic.put("transactions", transactionNum);
      updateStatistic.put("block_size", blockSize);
      updateStatistic.put("end", end);
      updateRequest.doc(updateStatistic.toJSONString(), XContentType.JSON);
      updateRequest.upsert(indexRequest);
      connectionTool.client.update(updateRequest, RequestOptions.DEFAULT);
      time = getMaxStatisticTime() + period;
      if (!end) {
        break;
      }
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
      }, 0, 2, TimeUnit.SECONDS);
  }
}
