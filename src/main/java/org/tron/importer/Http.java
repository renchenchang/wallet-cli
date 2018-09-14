package org.tron.importer;

import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

public class Http {


  public static String doPost(String url, String sql, String charset)
      throws IOException {

    HttpClientBuilder builder = HttpClients.custom();
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(4, false);
    builder.setConnectionManager(cm);
    builder.setRetryHandler(retryHandler);
    CloseableHttpClient httpClient = builder.build();

    String result = "";
    HttpPost httpPost = new HttpPost(url);
    httpPost.setHeader("Connection", "Close");
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(7000)
        .setConnectionRequestTimeout(7000).setSocketTimeout(7000).build();
    httpPost.setConfig(requestConfig);
    CloseableHttpResponse response = null;
    httpPost.setHeader("Cookie", "__utma=226521935.73826752.1323672782.1325068020.1328770420.6;");
    StringEntity postingString = new StringEntity(sql);//gson.tojson() converts your pojo to json
    httpPost.setEntity(postingString);
    httpPost.addHeader("content-type", "application/json");
    response = httpClient.execute(httpPost);
    HttpEntity resEntity = response.getEntity();
    result = EntityUtils.toString(resEntity, charset);
    return result;
  }
}