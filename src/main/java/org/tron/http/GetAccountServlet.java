package org.tron.http;

import com.alibaba.fastjson.JSONObject;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.tron.importer.JsonFormat;
import org.tron.importer.Util;
import org.tron.protos.Protocol.Account;
import org.tron.walletserver.WalletApi;

public class GetAccountServlet extends HttpServlet {

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    doPost(request, response);
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      String address = request.getParameter("address");
      Account account = WalletApi.queryAccount(WalletApi.decodeFromBase58Check(address));
      JSONObject json = JSONObject.parseObject(JsonFormat.printToString(account));
      if (json.containsKey("account_name")) {
        json.put("account_name", Util.hexToAscii(json.getString("account_name")));
      }
      if (json.containsKey("asset_issued_name")) {
        json.put("asset_issued_name", Util.hexToAscii(json.getString("asset_issued_name")));
      }
      response.getWriter().println(json);
    } catch (Exception e) {

    }
  }
}
