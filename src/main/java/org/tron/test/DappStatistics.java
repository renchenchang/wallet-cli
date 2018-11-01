package org.tron.test;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.tron.importer.Http;
import springfox.documentation.spring.web.json.Json;

public class DappStatistics {

  public static void main(String[] args) {
    String data = Http.doGet("https://dappradar.com/api/eos/dapps/theRest", "utf-8", 5);
    JSONObject json = JSONObject.parseObject(data);
    JSONArray array =  json.getJSONObject("data").getJSONArray("list");
    long lastDay = 0;
    long lastWeek = 0;
    for(int i=0; i<array.size(); i++) {
      JSONObject app = array.getJSONObject(i);
      lastDay += app.getLong("txLastDay");
      lastWeek += app.getLong("txLastWeek");
    }
    System.out.println("array.size()=" + array.size());
    System.out.println("txLastDay=" + lastDay);
    System.out.println("txLastWeek=" + lastWeek);
  }
}
