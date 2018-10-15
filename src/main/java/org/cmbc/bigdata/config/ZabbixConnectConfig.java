package org.cmbc.bigdata.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Configuration
@Component
public class ZabbixConnectConfig {
  @Value("${zabbix.host}")
  public String zabbixHost;

  @Value("${zabbix.serverport}")
  public int zabbixServerPort;

  @Value("${zabbix.apiport}")
  public int zabbixApiPort;

  @Value("${zabbix.user}")
  public String zabbixUser;

  @Value("${zabbix.passwd}")
  public String zabbixPasswd;

  @Value("${zabbix.defaultHostgroup}")
  public String zabbixDefaultHostgroup;
}
