package org.cmbc.bigdata.service;

import com.alibaba.fastjson.JSONObject;
import io.github.hengyunabc.zabbix.api.DefaultZabbixApi;
import io.github.hengyunabc.zabbix.api.Request;
import io.github.hengyunabc.zabbix.api.RequestBuilder;
import io.github.hengyunabc.zabbix.api.ZabbixApi;
import io.github.hengyunabc.zabbix.sender.DataObject;
import io.github.hengyunabc.zabbix.sender.SenderResult;
import io.github.hengyunabc.zabbix.sender.ZabbixSender;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.cmbc.bigdata.config.ZabbixConnectConfig;
import org.cmbc.bigdata.model.RestResult;
import org.cmbc.bigdata.model.ResultCode;
import org.cmbc.bigdata.model.ZabbixMetric;
import org.cmbc.bigdata.model.ZabbixModel;
import org.cmbc.bigdata.utils.ZabbixApi3;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
@Getter
@Setter
@Service
public class ZabbixService {
  private ZabbixApi3 zabbixApi;
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
      zabbixApi = new ZabbixApi3(url);
      zabbixApi.init();
    }
  }

  public void destroyZabbixApi() {
    if (zabbixApi != null) {
      zabbixApi.destroy();
      zabbixApi = null;
    }
  }

  public RestResult send(List<DataObject> dataObjectList) {
    this.initZabbixSender();
    log.info("Connect to Zabbix host =" + this.zabbixSender.getHost() +
            ", port=" + this.zabbixSender.getPort());
    RestResult restResult;
    try {
      SenderResult senderResult = this.zabbixSender.send(dataObjectList);
      if (senderResult.success()) {
        restResult = RestResult.success(senderResult, ResultCode.SUCCESS.message());
      } else {
        String msg = "Sent Total:" + senderResult.getTotal() + ", Processed:" +
                senderResult.getProcessed() + ", Failed:" + senderResult.getFailed();
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

  public RestResult sendP2(List<ZabbixMetric> zabbixMetrics) {
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
    HashMap<String, HashMap> checkData = new HashMap();
    initZabbixApi();

    boolean login = zabbixApi.login(zabbixConnectConfig.zabbixUser, zabbixConnectConfig.zabbixPasswd);
    if (!login) {
      log.error("Login Zabbix failure.");
      restResult = RestResult.failure(ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR,
              ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR.message(),
              ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR.message());
      return restResult;
    }

    for (ZabbixMetric zabbixMetric : zabbixMetrics) {
      String host = zabbixMetric.getHost();
      String key = zabbixMetric.getItemKey();
      Map hostSearchResponse = zabbixApi.getHostIdByName(host);
      String hostid = null;
      String itemid = null;
      if (hostSearchResponse.containsKey("error")) {
        HashMap msgMap = new HashMap();
        msgMap.put("error", hostSearchResponse.get("error"));
        checkData.put(host, msgMap);
        continue;
      } else {
        hostid = (String) hostSearchResponse.get("hostid");
      }
      if (hostid != null) {
        Map itemSearchResponse = zabbixApi.getItemIdByKeyAndHostId(key, hostid);
        if (itemSearchResponse.containsKey("error")) {
          HashMap errMap = new HashMap();
          errMap.put("error", itemSearchResponse.get("error"));
          errMap.put("msg", "Search Item Failure.");
          checkData.put(host, errMap);
          continue;
        } else {
          itemid = (String) itemSearchResponse.get("itemid");
        }
      }
      if (!(hostid != null && itemid != null)) {
        if (!checkData.containsKey(host)) {
          HashMap existMap = new HashMap();
          existMap.put("hostid", hostid);
          checkData.put(host, existMap);
        } else {
          checkData.get(host).put("hostid", hostid);
        }
        List<ZabbixMetric> itemList = new ArrayList<>();
        if (checkData.get(host).containsKey("items")) {
          itemList = (List) checkData.get(host).get("items");
        }
        itemList.add(zabbixMetric);
        checkData.get(host).put("items", itemList);
      }
    }

    restResult = RestResult.success(checkData,
            "Refer to data field to get invalid info. hostid=null means hostname does not exist in zabbix. You need create host before sending data. " +
                    "hostid!=null means item keys in field 'items' do not exist in zabbix. You need create items before sending data.");
    destroyZabbixApi();

    return restResult;
  }

  public RestResult createHostItem(HashMap<String, ZabbixModel> zabbixModels) {
    RestResult restResult = null;
    Map<String, HashMap> response = new HashMap();
    initZabbixApi();
    boolean login = zabbixApi.login(zabbixConnectConfig.zabbixUser, zabbixConnectConfig.zabbixPasswd);
    if (!login) {
      log.error("Login Zabbix failure.");
      restResult = RestResult.failure(ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR,
              ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR.message(),
              ResultCode.INTERFACE_ZABBIX_LOGIN_ERROR.message());
      return restResult;
    }
    for (Map.Entry<String, ZabbixModel> entry: zabbixModels.entrySet()) {
      String host = entry.getKey();
      ZabbixModel zabbixModel = entry.getValue();
      String hostid = zabbixModel.getHostid();
      List<ZabbixMetric> items = zabbixModel.getItems();
      ArrayList itemsCreateInfo = new ArrayList<>();
      response.put(host, new HashMap());
      if (hostid == null) {
        //When host not exists, create host first. Ensure the host group should be created also.
        String hostgroup = null;
        String hostgroupId = null;
        if (items.size() > 0) {
          hostgroup = items.get(0).getHostgroup();
        }
        if (hostgroup == null) {
          hostgroup = zabbixConnectConfig.zabbixDefaultHostgroup;
        }
        //If hostGroup is specified, use it.
        Map searchGroupResponse = zabbixApi.getGroupIdByName(hostgroup);
        if (!searchGroupResponse.containsKey("error")) {
          hostgroupId = (String)searchGroupResponse.get("groupid");
        }
        //If hostGroup not exists, create it.
        if (hostgroupId == null) {
          Map hostgroupCreateResponse = zabbixApi.hostgroupCreateV3(hostgroup);
          if (hostgroupCreateResponse.containsKey("error")) {
            response.get(host).put("hostgroupid", null);
            response.get(host).put("msg", "Host Group Create Failure.");
            response.get(host).put("error", hostgroupCreateResponse.get("error"));
          } else {
            hostgroupId = (String)hostgroupCreateResponse.get("groupid");
          }
        }
        if (hostgroupId != null) {
          //Create host.
          Map hostCreateResponse = zabbixApi.hostCreateV3(host, hostgroupId);
          if (hostCreateResponse.containsKey("error")) {
            response.get(host).put("msg", "Host Create failure.");
            response.get(host).put("error", hostCreateResponse.get("error"));
            response.get(host).put("hostid", null);
          } else {
            hostid = (String)hostCreateResponse.get("hostid");
          }
        }
      } else {
        Map hostSearchResponse = zabbixApi.getHostIdByName(host);
        String currentHostId = null;
        if (hostSearchResponse.containsKey("error")) {
          response.get(host).put("error", hostSearchResponse.get("error"));
        } else {
          currentHostId = (String)hostSearchResponse.get("hostid");
        }
        log.info("host:" + host + ",hostid:" + hostid + ", curretHostId:" + currentHostId);
        if (!hostid.equals(currentHostId)) {
          log.info("host:" + host + " hostid has changed.");
        }
        hostid = currentHostId;
      }
      response.get(host).put("hostid", hostid);
      if (hostid != null) {
        //Create item.
        for (int i = 0; i < items.size(); i++) {
          Map itemCreateResponse = zabbixApi.itemCreateFromMetric(hostid, items.get(i));
          itemCreateResponse.put("itemName", items.get(i).getItemName());
          itemCreateResponse.put("itemKey", items.get(i).getItemKey());
          if (itemCreateResponse.containsKey("error")) {
            itemCreateResponse.put("created", false);
            itemCreateResponse.put("msg", "Item Create failure.");
            itemCreateResponse.put("error", itemCreateResponse.get("error"));
          } else {
            itemCreateResponse.put("created", true);
            itemCreateResponse.put("itemid", itemCreateResponse.get("itemid"));
          }
          itemsCreateInfo.add(itemCreateResponse);
          response.get(host).put("items", itemsCreateInfo);
        }

      }
    }

    restResult = RestResult.success(response, "Refer to data field to check items create info.");
    destroyZabbixApi();

    return restResult;
  }

  public void testZabbixApiAndHttpClient() {
    RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(5 * 1000).setConnectionRequestTimeout(5 * 1000)
            .setSocketTimeout(5 * 1000).build();
    PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();

    CloseableHttpClient httpclient = HttpClients.custom().setConnectionManager(connManager)
            .setDefaultRequestConfig(requestConfig).build();

    ZabbixApi zabbixApi = new DefaultZabbixApi(
            "http://127.0.0.1:8888/zabbix/api_jsonrpc.php", httpclient);
    zabbixApi.init();

    String apiVersion = zabbixApi.apiVersion();
    System.out.println("api version:" + apiVersion);

    zabbixApi.destroy();
  }

  public void testZabbixApi() {
    String url = "http://127.0.0.1:8888/zabbix/api_jsonrpc.php";
    DefaultZabbixApi zabbixApi = new DefaultZabbixApi(url);
    zabbixApi.init();

    boolean login = zabbixApi.login("Admin", "zabbix");
    System.err.println("login:" + login);

    String host = "127.0.0.1";
    JSONObject filter = new JSONObject();

    System.out.println("zabbixhost Exists:" + zabbixApi.hostExists("wenqiaodeMacBook-Pro-2.local"));
    filter.put("host", new String[] { host });
    Request getRequest = RequestBuilder.newBuilder()
            .method("host.get")
            .build();
    JSONObject getResponse = zabbixApi.call(getRequest);
    System.out.println(getResponse);
    int hostNum = getResponse.getJSONArray("result").size();
    System.out.println("hostNum = " + hostNum);
    String hostid = getResponse.getJSONArray("result")
            .getJSONObject(0).getString("hostid");
    System.out.println(getResponse.getJSONArray("result").getJSONObject(0).toJSONString());
  }

  public void testZabbixSender() throws IOException {
    String host = "127.0.0.1";
    int port = 10051;
    ZabbixSender zabbixSender = new ZabbixSender(host, port);

    DataObject dataObject = new DataObject();
    dataObject.setHost("wenqiaodeMacBook-Pro-2.local");
    dataObject.setKey("trapper");
    dataObject.setValue(Long.toString(System.currentTimeMillis()));
    dataObject.setClock(System.currentTimeMillis()/1000);
    DataObject dataObjectWrong = new DataObject();
    dataObjectWrong.setHost("wenqiaodeMacBook-Pro-2");
    dataObjectWrong.setKey("trapper");
    dataObjectWrong.setValue(Long.toString(System.currentTimeMillis()));
    // TimeUnit is SECONDS.
    dataObjectWrong.setClock(System.currentTimeMillis()/1000);
    List<DataObject> dataObjectList = new ArrayList();
    dataObjectList.add(dataObject);
    dataObjectList.add(dataObjectWrong);
    //SenderResult result = zabbixSender.send(dataObject);
    SenderResult result = zabbixSender.send(dataObjectList);

    System.out.println("result:" + result);
    if (result.success()) {
      System.out.println("send success.");
    } else {
      System.err.println("send fail!");
    }
  }
}
