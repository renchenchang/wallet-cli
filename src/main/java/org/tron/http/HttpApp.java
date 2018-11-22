package org.tron.http;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.tron.walletserver.WalletApi;

public class HttpApp {

  public static void main(String[] args) throws Exception {
    Server server = new Server(WalletApi.httpPort);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/tron/");
    server.setHandler(context);

    ServletHolder exchangeHolder = new ServletHolder(new GetExchangeServlet());
    exchangeHolder.setInitOrder(1);
    context.addServlet(exchangeHolder, "/exchange-real-price");

    ServletHolder exchangeHistoryHolder = new ServletHolder(new GetExchangeHistoryServlet());
    exchangeHistoryHolder.setInitOrder(2);
    context.addServlet(exchangeHistoryHolder, "/exchange-history");

    context.addServlet(new ServletHolder(new GetAccountServlet()), "/account");

    server.start();
    server.join();
  }
}
