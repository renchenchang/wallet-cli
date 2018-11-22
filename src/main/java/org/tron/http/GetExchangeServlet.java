package org.tron.http;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.tron.protos.Protocol.Exchange;
import org.tron.walletserver.WalletApi;


public class GetExchangeServlet extends HttpServlet {

  private HashMap<String, String> exchanges = new HashMap();
  private Timer timer = new Timer();

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      JSONArray data = new JSONArray();
      for (Entry<String, String> ent : exchanges.entrySet()) {
        data.add(JSONObject.parse(ent.getValue()));
      }
      response.getWriter().println(data.toJSONString());
    } catch (Exception e) {

    }
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    doPost(request, response);
  }

  private void updateExchanges() {
    for (Exchange exchange : WalletApi.listExchanges().get().getExchangesList()) {
      JSONObject json = new JSONObject();
      json.put("first_token_id", exchange.getFirstTokenId().toStringUtf8());
      json.put("first_token_balance", exchange.getFirstTokenBalance());
      json.put("second_token_id", exchange.getSecondTokenId().toStringUtf8());
      json.put("second_token_balance", exchange.getSecondTokenBalance());
      json.put("id", exchange.getExchangeId());
      exchanges.put(exchange.getExchangeId() + "", json.toJSONString());
    }
    System.out.println("exchange updated at " + new Date());
  }

  @Override
  public void init() {
    timer.schedule(new TimerTask() {
      public void run() {
        updateExchanges();
      }
    }, 0, 10000);
  }
}