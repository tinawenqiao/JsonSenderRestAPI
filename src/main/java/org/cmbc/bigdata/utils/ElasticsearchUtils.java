package org.cmbc.bigdata.utils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.cmbc.bigdata.config.ESConnectConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

@Slf4j
@Getter
@Setter
@Component
public class ElasticsearchUtils {

  @Autowired
  private ESConnectConfig esConnectConfig;

  @Value("${es.authentication}")
  public boolean authentication;

  @Value("${es.user}")
  public String esUser;

  @Value("${es.passwd}")
  public String esPasswd;

  @Value("${es.addresses}")
  public String addresses;
  private RestClient restClient;

  public void init() {
    restClient = createRestClient();
  }

  public void destroy() {
    try {
      if (restClient != null) {
        restClient.close();
      }
    } catch (IOException ioException) {
      log.error("Close RestClient Error.", ioException);
    }
  }

  private RestClient createRestClient() {
    HttpHost[] hosts = new HttpHost[getAddresses().size()];
    int i = 0;
    for (String address : getAddresses()) {
      try {
        URL url = new URL(address);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException malformedURLException) {
        log.error("Elasticsearch Addresses Config error.");
        return null;
      }
    }

    RestClientBuilder builder = RestClient.builder(hosts);

    if (authentication) {
      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(esUser, esPasswd));
      builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      });
    }

    return builder.build();
  }

  private ArrayList<String> getAddresses() {
    ArrayList<String> result = new ArrayList<>();
    String esAddresses = esConnectConfig.getAddresses();
    String[] addressesArray = esAddresses.split(",");

    for (String hostAndPort : addressesArray) {
      result.add(String.format("http://%s", hostAndPort));
    }

    return result;
  }
}
