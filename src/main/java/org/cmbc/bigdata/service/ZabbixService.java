package org.cmbc.bigdata.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hengyunabc.zabbix.sender.DataObject;
import io.github.hengyunabc.zabbix.sender.SenderResult;
import io.github.hengyunabc.zabbix.sender.ZabbixSender;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.cmbc.bigdata.config.ZabbixConnectConfig;
import org.cmbc.bigdata.model.RestResult;
import org.cmbc.bigdata.model.ResultCode;
import org.cmbc.bigdata.model.ZabbixMetric;
import org.cmbc.bigdata.model.ZabbixModel;
import org.cmbc.bigdata.zabbix.ZabbixAPIResult;
import org.cmbc.bigdata.zabbix.ZabbixApi;
import org.cmbc.bigdata.zabbix.ZabbixItemType;
import org.cmbc.bigdata.zabbix.ZabbixItemValueType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Getter
@Setter
@Service
@Component
public class ZabbixService {
  private ZabbixApi zabbixApi;
  private ZabbixSender zabbixSender;
  @Autowired
  private ZabbixConnectConfig zabbixConnectConfig;

  public void initZabbixSender() {
    if (zabbixSender == null) {
      zabbixSender = new ZabbixSender(zabbixConnectConfig.zabbixHost, zabbixConnectConfig.zabbixServerPort);
    }
  }

  public void initZabbixApi() {
    if (zabbixApi == null) {
      String url = String.format("http://%s:%d/zabbix/api_jsonrpc.php",
              zabbixConnectConfig.zabbixHost, zabbixConnectConfig.zabbixApiPort);
      zabbixApi = new ZabbixApi(url);
      zabbixApi.init();
    }
  }

  public void destroyZabbixApi() {
    if (zabbixApi != null) {
      zabbixApi.destroy();
      zabbixApi = null;
    }
  }

  public boolean loginZabbix() {
    return zabbixApi.login(zabbixConnectConfig.zabbixUser, zabbixConnectConfig.zabbixPasswd);
  }

  public RestResult getLoginFailure() {
    log.error("Login Zabbix failure.");
    RestResult restResult = RestResult.failure(ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR,
            ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR.message(),
            ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR.message());

    return restResult;
  }

  public RestResult send(List<DataObject> dataObjectList) {
    log.info("Request data:{}", dataObjectList.toString());
    this.initZabbixSender();
    log.info("Connect to Zabbix host:{}, port:{}", this.zabbixSender.getHost(), this.zabbixSender.getPort());
    RestResult restResult = new RestResult();
    try {
      SenderResult senderResult = this.zabbixSender.send(dataObjectList);
      if (senderResult.success()) {
        restResult = RestResult.success(senderResult, ResultCode.SUCCESS.message());
      } else {
        String msg = "Sent Total:" + senderResult.getTotal() + ", Processed:" +
                senderResult.getProcessed() + ", Failed:" + senderResult.getFailed();
        log.info(msg);
        if (senderResult.getProcessed() > 0) {
          restResult = RestResult.failure(ResultCode.INTERFACE_ZABBIX_SENDER_PARTIAL_FAILURE,
                  ResultCode.INTERFACE_ZABBIX_SENDER_PARTIAL_FAILURE.message(), msg);
        } else {
          restResult = RestResult.failure(ResultCode.INTERFACE_ZABBIX_SENDER_ALL_FAILURE,
                  ResultCode.INTERFACE_ZABBIX_SENDER_ALL_FAILURE.message(), msg);
        }
        restResult.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (IOException ioException) {
      restResult = RestResult.failure(ResultCode.INTERFACE_ZABBIX_SENDER_ERROR,
              ResultCode.INTERFACE_ZABBIX_SENDER_ERROR.message(), ioException.getMessage());
      restResult.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    return restResult;
  }

  public RestResult sendDerivedFormat(List<ZabbixMetric> zabbixMetrics) {
    List<DataObject> dataObjectList = transP2ToP1(zabbixMetrics);

    return send(dataObjectList);
  }

  private List<DataObject> transP2ToP1(List<ZabbixMetric> zabbixMetrics) {
    List<DataObject> dataObjectList = new ArrayList<>();
    zabbixMetrics.forEach(zabbixMetric -> {
      long lock = (Timestamp.valueOf(zabbixMetric.getTimestamp()).getTime()) / 1000;
      String host = zabbixMetric.getHost();
      String key = zabbixMetric.getItemKey();
      String value = zabbixMetric.getValue();
      DataObject dataObject = new DataObject(lock, host, key, value);
      dataObjectList.add(dataObject);
    });

    return dataObjectList;
  }

  public RestResult checkZabbixData(List<ZabbixMetric> zabbixMetrics) {
    RestResult restResult;
    Map<String, ZabbixModel> checkData = new HashMap<>();

    initZabbixApi();
    if (!loginZabbix()) return getLoginFailure();

    for (ZabbixMetric zabbixMetric : zabbixMetrics) {
      String host = zabbixMetric.getHost();
      String itemKey = zabbixMetric.getItemKey();
      String hostid = null;
      String itemid = null;

      ZabbixAPIResult hostGetResult = zabbixApi.hostGetByHostName(host);
      if (!hostGetResult.isFail()) {
        JsonNode hostArray = (JsonNode) hostGetResult.getData();
        if (hostArray.size() > 0) {
          hostid = hostArray.get(0).get("hostid").asText();
        }
      }

      if (hostid != null) {
        ArrayList<String> itemKeyList = new ArrayList<>();
        itemKeyList.add(itemKey);
        ZabbixAPIResult itemGetResult = zabbixApi.itemGetByHostNameAndItemKey(host, itemKeyList);
        if (!itemGetResult.isFail()) {
          JsonNode itemArray = (JsonNode) itemGetResult.getData();
          if (itemArray.size() > 0) {
            hostid = itemArray.get(0).get("hostid").asText();
          }
        }
      }
      if (!(hostid != null && itemid != null)) {
        if (!checkData.containsKey(host)) {
          //Map<String, String> hostMap = new HashMap();
          ZabbixModel hostMap = new ZabbixModel();
          hostMap.setHostid(hostid);
          checkData.put(host, hostMap);
        } else {
          checkData.get(host).setHostid(hostid);
        }
        List<ZabbixMetric> itemList = new ArrayList<>();
        if (checkData.get(host).getItems() != null) {
          itemList = checkData.get(host).getItems();
        }
        itemList.add(zabbixMetric);
        checkData.get(host).setItems(itemList);
      }
    }

    restResult = RestResult.success(checkData,
            "Refer to data field to get invalid info. hostid=null means hostname does not exist in zabbix. You need create host before sending data. " +
                    "hostid!=null means item keys in field 'items' do not exist in zabbix. You need create items before sending data.");
    destroyZabbixApi();

    return restResult;
  }

  public RestResult createHostItem(Map<String, ZabbixModel> zabbixModels) {
    RestResult restResult;
    Map<String, HashMap> response = new HashMap<>();

    initZabbixApi();
    if (!loginZabbix()) return getLoginFailure();

    for (Map.Entry<String, ZabbixModel> entry : zabbixModels.entrySet()) {
      String host = entry.getKey();
      ZabbixModel zabbixModel = entry.getValue();
      String hostid = zabbixModel.getHostid();
      List<ZabbixMetric> items = zabbixModel.getItems();
      response.put(host, new HashMap());

      if (!zabbixApi.hostExists(host)) {
        //If host not exists, create host first. Ensure the host group should be created also.
        String hostgroup = null;
        String hostgroupId = null;
        if (items.size() > 0) {
          //If hostGroup is specified, use it.
          hostgroup = items.get(0).getHostgroup();
        }
        //If host group is not specified, use the default host group
        hostgroup = (hostgroup == null) ? (zabbixConnectConfig.zabbixDefaultHostgroup) : hostgroup;

        //If host group not exists, create it.
        if (!zabbixApi.hostgroupExists(hostgroup)) {
          ZabbixAPIResult hostgroupCreateResult = zabbixApi.hostgroupCreate(hostgroup);
          if (hostgroupCreateResult.isFail()) {
            log.info("Create host group:{} failed.", hostgroup);
            response.get(host).put("code", ResultCode.INTERFACE_ZABBIX_CREATE_HOSTGROUP_FAILURE.code());
            response.get(host).put("msg", ResultCode.INTERFACE_ZABBIX_CREATE_HOSTGROUP_FAILURE.message());
            continue;
          }
          log.info("Create host group:{} successfully.", hostgroup);
          JsonNode hostgroupids = (JsonNode) hostgroupCreateResult.getData();
          if (hostgroupids.get("groupids").size() > 0) {
            hostgroupId = hostgroupids.get("groupids").get(0).asText();
          }
        } else {
          //Get host group id
          ZabbixAPIResult hostgroupGetResult = zabbixApi.hostgroupGetByGroupName(hostgroup);
          if (hostgroupGetResult.isFail()) {
            log.info("Search host group:{} failed.", hostgroup);
            response.get(host).put("code", ResultCode.INTERFACE_ZABBIX_SEARCH_HOSTGROUP_FAILURE.code());
            response.get(host).put("msg", ResultCode.INTERFACE_ZABBIX_SEARCH_HOSTGROUP_FAILURE.message());
            continue;
          }
          JsonNode hostgroupArray = (JsonNode) hostgroupGetResult.getData();
          if (hostgroupArray.size() > 0) {
            hostgroupId = hostgroupArray.get(0).get("groupid").asText();
          }
        }

        if (hostgroupId != null) {
          //Create host interface and host.
          ArrayList<String> groupIdList = new ArrayList();
          groupIdList.add(hostgroupId);
          ZabbixAPIResult hostCreateResult = zabbixApi.hostCreate(host, groupIdList, createHostInterface("10050", "1"));
          log.info("Create host:{}, groupId:{}.", host, hostgroupId);
          if (hostCreateResult.isFail()) {
            log.info("Create host :" + host + " failed.");
            response.get(host).put("code", ResultCode.INTERFACE_ZABBIX_CREATE_HOST_FAILURE.code());
            response.get(host).put("msg", ResultCode.INTERFACE_ZABBIX_CREATE_HOST_FAILURE.message());
            continue;
          }
        }
      }

      ZabbixAPIResult hostGetResult = zabbixApi.hostGetByHostName(host);
      String currentHostId = null;
      if (hostGetResult.isFail()) {
        log.info("Search host :{} failed.", host);
        response.get(host).put("code", ResultCode.INTERFACE_ZABBIX_SEARCH_HOST_FAILURE.code());
        response.get(host).put("msg", ResultCode.INTERFACE_ZABBIX_SEARCH_HOST_FAILURE.message());
        continue;
      }
      JsonNode hostArray = (JsonNode) hostGetResult.getData();
      if (hostArray.size() > 0) {
        currentHostId = hostArray.get(0).get("hostid").asText();
      }
      log.info("host:{}, hostid:{}, currentHostId:{}.", host, hostid, currentHostId);
      //If hostid changed
      if (hostid != null && !hostid.equals(currentHostId)) {
        log.info("host:{} hostid has changed.", host);
      }

      hostid = currentHostId;
      response.get(host).put("hostid", hostid);

      //Create items.
      String interfaceId = null;
      ArrayList<String> hostIdList = new ArrayList<>();
      hostIdList.add(hostid);
      ZabbixAPIResult interfaceGetResult = zabbixApi.hostInterfaceGetByHostIds(hostIdList);
      if (interfaceGetResult.isFail()) {
        log.info("Search interface of host :" + host + " failed.");
        response.get(host).put("code", ResultCode.INTERFACE_ZABBIX_SEARCH_INTERFACE_FAILURE.code());
        response.get(host).put("msg", ResultCode.INTERFACE_ZABBIX_SEARCH_INTERFACE_FAILURE.message());
        continue;
      }
      JsonNode interfaceArray = (JsonNode) interfaceGetResult.getData();
      if (interfaceArray.size() > 0) {
        interfaceId = interfaceArray.get(0).get("interfaceid").asText();
      }

      ArrayList<HashMap<String, Object>> paramList = new ArrayList<>();
      for (int i = 0; i < items.size(); i++) {
        HashMap<String, Object> param = new HashMap();
        param.put("delay", "60");
        param.put("hostid", hostid);
        param.put("interfaceid", interfaceId);
        param.put("key_", items.get(i).getItemKey());
        param.put("name", items.get(i).getItemName());
        param.put("type", ZabbixItemType.ZABBIX_TRAPPER.code());
        param.put("value_type", ZabbixItemValueType.TEXT.code());

        paramList.add(param);
      }

      ZabbixAPIResult itemsCreateResult = zabbixApi.itemListCreate(paramList);
      if (itemsCreateResult.isFail()) {
        log.info("Create items failed.");
        response.get(host).put("code", ResultCode.INTERFACE_ZABBIX_CREATE_ITEM_FAILURE.code());
        response.get(host).put("msg", itemsCreateResult.getData());
        continue;
      }
      response.get(host).put("items", itemsCreateResult.getData());
    }

    restResult = RestResult.success(response, "Refer to data field to check items create info.");
    destroyZabbixApi();

    return restResult;
  }

  private Map createHostInterface(String port, String type) {
    Map hostInterface = new HashMap<>();
    hostInterface.put("dns", "");
    hostInterface.put("ip", "127.0.0.1");
    hostInterface.put("main", 1);
    hostInterface.put("port", port);
    hostInterface.put("type", type);
    hostInterface.put("useip", 1);

    return hostInterface;
  }
}
