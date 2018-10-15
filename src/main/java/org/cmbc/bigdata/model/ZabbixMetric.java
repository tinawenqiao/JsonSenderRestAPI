package org.cmbc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ZabbixMetric {
  // Metric fetch time. Format:yyyy-MM-dd HH:mm:ss
  public String timestamp;
  public String host;
  public String hostgroup;
  // Mapping to item name.
  public String itemName;
  // Mapping to item key.
  public String itemKey;
  public String value;
  // Mapping to item Application.
  //public String itemApplication;
}
