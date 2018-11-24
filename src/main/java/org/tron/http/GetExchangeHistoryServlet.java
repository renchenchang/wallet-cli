package org.tron.http;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Date;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.tron.importer.ConnectionTool;
import org.tron.importer.Util;
import org.tron.protos.Protocol.Exchange;
import org.tron.walletserver.WalletApi;

public class GetExchangeHistoryServlet extends HttpServlet {

  private ConnectionTool connectionTool = new ConnectionTool();
  HashMap<String, JSONArray> priceHistory = new HashMap<>();
  private Timer timer = new Timer();

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      String exchangeID = request.getParameter("id");
      JSONArray array = priceHistory.get(exchangeID);
      int size = array.size();
      if (request.getParameter("size") != null) {
        size = Integer.parseInt(request.getParameter("size"));
      }
      int start = (size > array.size()) ? 0 : (array.size() - size);
      JSONArray reply = new JSONArray();
      for (int i= start; i<array.size(); i++) {
        reply.add(array.getJSONObject(i));
      }
      response.getWriter().println(reply.toJSONString());
    } catch (Exception e) {

    }
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    doPost(request, response);
  }

  private void updateExchangeHistory() {
    int maxLen = 700;
    for (Exchange exchange : WalletApi.listExchanges().get().getExchangesList()) {
      JSONArray list = new JSONArray();
      String id = Long.toString(exchange.getExchangeId());
      if (priceHistory.containsKey(id)) {
        list = priceHistory.get(id);
      }
      JSONObject json = new JSONObject();
      json.put("first_token_id", exchange.getFirstTokenId().toStringUtf8());
      json.put("first_token_balance", exchange.getFirstTokenBalance());
      json.put("second_token_id", exchange.getSecondTokenId().toStringUtf8());
      json.put("second_token_balance",  exchange.getSecondTokenBalance());
      json.put("date_created", System.currentTimeMillis());
      list.add(json);
      JSONArray newList = new JSONArray();
      if (list.size() > maxLen) {
        for (int i=maxLen; i >=0; i--) {
          newList.add(list.get(list.size() - i - 1));
        }
      } else {
        newList = list;
      }
      priceHistory.put(id, newList);
    }
    System.out.println("exchange history updated at " + new Date());
  }

  @Override
  public void init() {
    try {
      priceHistory = connectionTool.getExchangeHistory(System.currentTimeMillis()
          - 24 * 60 * 60 * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    timer.schedule(new TimerTask() {
      public void run() {
        updateExchangeHistory();
      }
    }, 120000, 120000);
  }
}
