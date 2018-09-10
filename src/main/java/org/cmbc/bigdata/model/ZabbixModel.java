package org.cmbc.bigdata.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class ZabbixModel {
  public String hostid;
  public List<ZabbixMetric> items;
}
