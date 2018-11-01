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
import java.util.Collections;
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
    RestResult restResult = elasticsearchService.write(esModel.getIndex(), esModel.getType(), esModel.getData());

    return restResult;
  }

  @PostMapping("/zabbix/list/format/original")
  @ApiOperation(value = "Send list of data to zabbix in the format of original format.")
  public RestResult sendListToZabbixInOriginalFormat(@RequestBody@Valid List<DataObject> zabbixData) {
    return zabbixService.send(zabbixData);
  }

  @PostMapping("/zabbix/one/format/original")
  @ApiOperation(value = "Send one to zabbix in the format of original format.")
  public RestResult sendToZabbixInOriginalFormat(@RequestBody@Valid DataObject zabbixData) {
    return zabbixService.send(Collections.singletonList(zabbixData));
  }

  @PostMapping("/zabbix/list/format/derived")
  @ApiOperation(value = "Send list of data to zabbix in the format of derived format.")
  public RestResult sendListToZabbixInDerivedFormat(@RequestBody@Valid List<ZabbixMetric> zabbixMetrics) {
    return zabbixService.sendDerivedFormat(zabbixMetrics);
  }

  @PostMapping("/zabbix/one/format/derived")
  @ApiOperation(value = "Send one to zabbix in the format of derived format.")
  public RestResult sendToZabbixInDerivedFormat(@RequestBody@Valid ZabbixMetric zabbixMetric) {
    return zabbixService.sendDerivedFormat(Collections.singletonList(zabbixMetric));
  }

  @PostMapping("/zabbix/check/list")
  @ApiOperation(value = "Check whether hosts and items exist.")
  public RestResult checkListOfZabbixData(@RequestBody List<ZabbixMetric> zabbixMetrics) {
    return zabbixService.checkZabbixData(zabbixMetrics);
  }

  @PostMapping("/zabbix/check/one")
  @ApiOperation(value = "Check whether host and item exist.")
  public RestResult checkZabbixData(@RequestBody ZabbixMetric zabbixMetric) {
    return zabbixService.checkZabbixData(Collections.singletonList(zabbixMetric));
  }

  @PostMapping("/zabbix/items")
  public RestResult zabbixCreateItem(@RequestBody HashMap<String, ZabbixModel> zabbixModels) {
    return zabbixService.createHostItem(zabbixModels);
  }
}
