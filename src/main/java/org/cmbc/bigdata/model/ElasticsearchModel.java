package org.cmbc.bigdata.model;

import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.util.Map;

@Getter
@Setter
public class ElasticsearchModel {
  @NotNull(message = "Index can not be null.")
  public String index;
  @NotNull(message = "Type can not be null.")
  public String type;
  @NotNull(message = "Doc can not be null")
  public Map doc;
}
