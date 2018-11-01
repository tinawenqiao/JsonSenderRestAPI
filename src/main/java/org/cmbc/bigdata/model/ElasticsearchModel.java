package org.cmbc.bigdata.model;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;

@Getter
@Setter
public class ElasticsearchModel {
  @NotNull(message = "Index can not be null.")
  public String index;
  @NotNull(message = "Type can not be null.")
  public String type;
  @NotNull(message = "Data can not be null")
  public Object data;
}
