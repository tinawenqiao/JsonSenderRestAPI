package org.cmbc.bigdata.config;

import lombok.Getter;
import lombok.Setter;
import org.cmbc.bigdata.utils.ElasticsearchUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@Configuration
public class ESConnectConfig {
  @Value("${es.addresses}")
  public String addresses;


  @Bean(initMethod = "init", destroyMethod = "destroy")
  public ElasticsearchUtils elasticsearchUtils()
  {
    return new ElasticsearchUtils();
  }

}
