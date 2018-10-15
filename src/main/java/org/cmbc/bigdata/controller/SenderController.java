package org.cmbc.bigdata.controller;

import io.github.hengyunabc.zabbix.sender.DataObject;
import io.swagger.annotations.ApiOperation;
import org.cmbc.bigdata.model.ElasticsearchModel;
import org.cmbc.bigdata.model.RestResult;
import org.cmbc.bigdata.model.ZabbixMetric;
import org.cmbc.bigdata.model.ZabbixModel;
import org.cmbc.bigdata.service.ElasticsearchService;
import org.cmbc.bigdata.service.ZabbixService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/v2/senders")
@Validated
public class SenderController {
  @Autowired
  private ElasticsearchService elasticsearchService;
  @Autowired
  private ZabbixService zabbixService;

  @GetMapping("/list")
  public String listSupport() {
    return "Now we support sending JSON data to elasticsearch and zabbix.";
  }

  @PostMapping("/elasticsearch")
  public RestResult sendToES(@RequestBody@Valid ElasticsearchModel esModel) {
    RestResult restResult = elasticsearchService.write(esModel.getIndex(), esModel.getType(), esModel.getDoc());

    return restResult;
  }

  @PostMapping("/zabbix/format/original")
  @ApiOperation(value = "Send data to zabbix in the format of list of original format.")
  public RestResult sendToZabbixInOriginalFormat(@RequestBody@Valid List<DataObject> zabbixData) {
    return zabbixService.send(zabbixData);
  }

  @PostMapping("/zabbix/format/derived")
  @ApiOperation(value = "Send data to zabbix in the format of list of derived format.")
  public RestResult sendToZabbixInDerivedFormat(@RequestBody@Valid List<ZabbixMetric> zabbixMetrics) {
    return zabbixService.sendDerivedFormat(zabbixMetrics);
  }

  @PostMapping("/zabbix/check")
  @ApiOperation(value = "Check whether host and item exist.")
  public RestResult checkZabbixData(@RequestBody List<ZabbixMetric> zabbixMetrics) {
    return zabbixService.checkZabbixData(zabbixMetrics);
  }

  @PostMapping("/zabbix/items")
  public RestResult zabbixCreateItem(@RequestBody HashMap<String, ZabbixModel> zabbixModels) {
    return zabbixService.createHostItem(zabbixModels);
  }
}
