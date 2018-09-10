package org.cmbc.bigdata.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.github.hengyunabc.zabbix.api.DefaultZabbixApi;
import io.github.hengyunabc.zabbix.api.Request;
import io.github.hengyunabc.zabbix.api.RequestBuilder;
import lombok.extern.log4j.Log4j;
import org.cmbc.bigdata.model.ZabbixItemType;
import org.cmbc.bigdata.model.ZabbixItemValueType;
import org.cmbc.bigdata.model.ZabbixMetric;

import java.util.HashMap;
import java.util.Map;

/*
DefaultZabbixApi has some functions only apply to Zabbi Api2, such as item.exists. Then we create ZabbixApi3.
*/
@Log4j
public class ZabbixApi3 extends DefaultZabbixApi {
  public ZabbixApi3(String url) {
    super(url);
  }

  public Map hostgroupCreateV3(String groupname) {
    System.out.println("+++Begin to create group:" + groupname);
    Map generalResponse = new HashMap();
    if (groupname == null || groupname.isEmpty()) {
      generalResponse.put("error", "group name is null or empty");
      return generalResponse;
    }
    Request request = RequestBuilder.newBuilder().method("hostgroup.create").paramEntry("name", groupname).build();
    JSONObject response = call(request);
    System.out.println("hostGroupCreateResult:" + response.toJSONString());

    if (response.containsKey("error")) {
      generalResponse.put("error", response.get("error"));
    } else {
      if (response.getJSONObject("result").getJSONArray("groupids").size() > 0) {
        generalResponse.put("groupid", response.getJSONObject("result").getJSONArray("groupids").getString(0));
      }
    }

    return generalResponse;
  }

  public Map getGroupIdByName(String groupname) {
    Map generalResponse = new HashMap();
    JSONObject filter = new JSONObject();
    filter.put("name", new String[] { groupname });

    Request request = RequestBuilder.newBuilder().method("hostgroup.get").paramEntry("filter", filter).build();
    JSONObject response = call(request);
    if (response.containsKey("error")) {
      generalResponse.put("error", response.get("error"));
    } else {
      if (response.getJSONArray("result").size() > 0) {
        System.out.println("hostgroup Search Result: " + response.getJSONArray("result"));
        String groupid = response.getJSONArray("result")
                .getJSONObject(0).getString("groupid");
        log.info("Found groupid:" + groupid + " for group:" + groupname);
        generalResponse.put("groupid", groupid);
      }
    }

    return generalResponse;
  }

  public Map hostCreateV3(String host, String groupId) {
    Map generalResponse = new HashMap();
    JSONObject hostInterface = new JSONObject();

    hostInterface.put("dns","");
    hostInterface.put("ip","127.0.0.1");
    hostInterface.put("main",1);
    hostInterface.put("port","10050");
    hostInterface.put("type",1);
    hostInterface.put("useip",1);

    JSONArray groups = new JSONArray();
    JSONObject group = new JSONObject();
    group.put("groupid", groupId);
    groups.add(group);
    Request request = RequestBuilder.newBuilder().method("host.create").paramEntry("host", host)
            .paramEntry("interfaces", hostInterface)
            .paramEntry("groups", groups).build();
    JSONObject response = call(request);

    if (response.containsKey("error")) {
      generalResponse.put("error", response.get("error"));
    } else {
      if (response.getJSONObject("result").getJSONArray("hostids").size() > 0) {
        generalResponse.put("hostid", response.getJSONObject("result").getJSONArray("hostids").getString(0));
      }
    }

    return generalResponse;
  }

  public Map getHostIdByName(String hostname) {
    Map generalResponse = new HashMap();
    JSONObject filter = new JSONObject();
    filter.put("host", new String[] { hostname });

    Request request = RequestBuilder.newBuilder().method("host.get").paramEntry("filter", filter).build();
    JSONObject response = call(request);
    if (response.containsKey("error")) {
      generalResponse.put("error", response.get("error"));
    } else {
      if (response.getJSONArray("result").size() > 0) {
        System.out.println("host Search Result: " + response.getJSONArray("result"));
        String hostid = response.getJSONArray("result")
                .getJSONObject(0).getString("hostid");
        log.info("Found hostid:" + hostid + " for host:" + hostname);
        generalResponse.put("hostid", hostid);
      }
    }

    return generalResponse;
  }

  public Map getItemIdByKeyAndHostId(String name, String hostid) {
    Map generalResponse = new HashMap();
    JSONObject filter = new JSONObject();
    filter.put("key_", new String[] { name });
    filter.put("hostid", new String[] { hostid });

    Request request = RequestBuilder.newBuilder().method("item.get").paramEntry("filter", filter).build();
    JSONObject response = call(request);
    if (response.containsKey("error")) {

    } else {
      if (response.getJSONArray("result").size() > 0) {
        System.out.println("item Search Result: " + response.getJSONArray("result"));
        String itemid = response.getJSONArray("result")
                .getJSONObject(0).getString("itemid");
        System.out.println("itemid=" + itemid);
        response.put("itemid", itemid);
      } else {
        log.info("item not found");
      }
    }

    return generalResponse;
  }

  public Map itemCreateFromMetric(String hostid, ZabbixMetric zabbixMetric) {
    Map generalResponse = new HashMap();

    Request request = RequestBuilder.newBuilder().method("item.create")
            .paramEntry("hostid", hostid)
            .paramEntry("key_", zabbixMetric.getItemKey())
            .paramEntry("name", zabbixMetric.getItemName())
            .paramEntry("type", ZabbixItemType.ZABBIX_TRAPPER.code())
            .paramEntry("value_type", ZabbixItemValueType.TEXT.code())
            .paramEntry("delay", 60).build();
    JSONObject response = call(request);
    System.out.println("itemCreateResult:" + response.toJSONString());

    if (response.containsKey("error")) {
      generalResponse.put("error", response.get("error"));
    } else {
      if (response.getJSONObject("result").getJSONArray("itemids").size() > 0) {
        generalResponse.put("itemid", response.getJSONObject("result").getJSONArray("itemids").getString(0));
      }
    }

    return generalResponse;
  }

  public static void main(String[] args) {
    String url = String.format("http://%s:%d/zabbix/api_jsonrpc.php",
            "127.0.0.1", 8888);
    //ZabbixApi3 zabbixApi3 = new ZabbixApi3(url);
    //zabbixApi3.init();
    //zabbixApi3.login("Admin", "zabbix");
    //zabbixApi3.getHostIdByName("wenqiaodeMacBook-Pro-2.local");
    //zabbixApi3.getItemIdByKeyAndHostId("trapper", "10108");
    //zabbixApi3.getGroupIdByName("Templates");
    //zabbixApi3.hostgroupCreateV3("hg4");
    //zabbixApi3.destroy();
  }
}
