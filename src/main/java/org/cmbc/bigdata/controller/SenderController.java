package org.cmbc.bigdata.controller;

import org.cmbc.bigdata.model.ElasticsearchModel;
import org.cmbc.bigdata.model.RestResult;
import org.cmbc.bigdata.service.ElasticsearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("/v2/senders")
@Validated
public class SenderController {
  @Autowired
  private ElasticsearchService elasticsearchService;

  @GetMapping("/support")
  public String listSupport() {
    return "Now we support sending JSON data to elasticsearch and zabbix.";
  }

  @PostMapping("/elasticsearch")
  public RestResult sendToES(@RequestBody@Valid ElasticsearchModel esModel) {
    RestResult restResult = elasticsearchService.write(esModel.getIndex(), esModel.getType(), esModel.getDoc());

    return restResult;
  }
}
