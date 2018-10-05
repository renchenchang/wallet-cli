package org.tron.importer;

import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

public class Http {


  public static String doPost(String url, String parameters, String charset)
      throws IOException {
    HttpClientBuilder builder = HttpClients.custom();
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(4, false);
    builder.setConnectionManager(cm);
    builder.setRetryHandler(retryHandler);
    CloseableHttpClient httpClient = builder.build();

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "rcc03083210"));
    HttpClientContext localContext = HttpClientContext.create();
    localContext.setCredentialsProvider(credentialsProvider);

    String result = "";
    HttpPost httpPost = new HttpPost(url);
    httpPost.setHeader("Connection", "Close");
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(7000)
        .setConnectionRequestTimeout(7000).setSocketTimeout(7000).build();
    httpPost.setConfig(requestConfig);
    CloseableHttpResponse response = null;
    httpPost.setHeader("Cookie", "__utma=226521935.73826752.1323672782.1325068020.1328770420.6;");
    StringEntity postingString = new StringEntity(parameters);//gson.tojson() converts your pojo to json
    httpPost.setEntity(postingString);
    httpPost.addHeader("content-type", "application/json");
    response = httpClient.execute(httpPost, localContext);
    HttpEntity resEntity = response.getEntity();
    result = EntityUtils.toString(resEntity, charset);
    return result;
  }

  public static String doGet(String url, String charset, int timeOut){
    HttpClientBuilder builder = HttpClients.custom();
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(4, false);
    builder.setConnectionManager(cm);
    builder.setRetryHandler(retryHandler);
    CloseableHttpClient httpClient = builder.build();
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(timeOut*1000).setConnectionRequestTimeout(timeOut*1000).setSocketTimeout(timeOut*1000).build();

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "rcc03083210"));
    HttpClientContext localContext = HttpClientContext.create();
    localContext.setCredentialsProvider(credentialsProvider);

    String result="";
    HttpGet request = null;
    CloseableHttpResponse response = null;
    try {
      request = new HttpGet(url);
      request.setConfig(requestConfig);
      request.setHeader("Connection", "Close");
      request.setHeader("Cookie","__utma=226521935.73826752.1323672782.1325068020.1328770420.6;");
      response = httpClient.execute(request, localContext);
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        result= EntityUtils.toString(response.getEntity(), charset);
      }
    } catch (ClientProtocolException e) {

    } catch (IOException e) {

    } finally {
      if(request != null) {
        request.abort();
        request.releaseConnection();
      }
      try {
        if(response != null){
          response.close();
        }
      } catch (IOException e) {

      }
      cm.close();
      try {
        httpClient.close();
      } catch (IOException e) {

      }
    }
    return result;
  }
}