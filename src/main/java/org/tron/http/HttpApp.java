package org.tron.http;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HttpApp {

  public static void main(String[] args) throws Exception {
    Server server = new Server(8095);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/exchanges/");
    server.setHandler(context);

    ServletHolder exchangeHolder = new ServletHolder(new GetExchangeServlet());
    exchangeHolder.setInitOrder(1);
    context.addServlet(exchangeHolder, "/real-price");

    ServletHolder exchangeHistoryHolder = new ServletHolder(new GetExchangeHistoryServlet());
    exchangeHistoryHolder.setInitOrder(2);
    context.addServlet(exchangeHistoryHolder, "/history");


    server.start();
    server.join();
  }
}
