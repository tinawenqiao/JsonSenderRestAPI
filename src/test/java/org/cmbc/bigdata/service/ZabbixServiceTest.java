package org.cmbc.bigdata.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hengyunabc.zabbix.sender.DataObject;
import io.github.hengyunabc.zabbix.sender.SenderResult;
import junit.framework.TestCase;
import org.cmbc.bigdata.model.RestResult;
import org.cmbc.bigdata.model.ResultCode;
import org.cmbc.bigdata.model.ZabbixMetric;
import org.cmbc.bigdata.model.ZabbixModel;
import org.cmbc.bigdata.zabbix.ZabbixAPIResult;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ZabbixServiceTest extends TestCase {
  @Autowired
  private ZabbixService zabbixService;
  private static RestResult checkResult;
  private static final String testGroup = "testGroup";
  private static final String testHost1 = "testHost1";
  private static final String testHost2 = "testHost2";
  private static final String itemKey1 = "itemKey1";
  private static final String itemName1 = "itemName1";
  private static final String itemKey2 = "itemKey2";
  private static final String itemName2 = "itemName2";

  static ArrayList<ZabbixMetric> metricList = new ArrayList<>();

  @Before
  public void before() {
    metricList = new ArrayList<>();

    ZabbixMetric metric1 = new ZabbixMetric(getTime(), testHost1, testGroup, itemName1, itemKey1,
            Double.toString(Math.random()));

    ZabbixMetric metric2 = new ZabbixMetric(getTime(), testHost1, testGroup, itemName2, itemKey2,
            Double.toString(Math.random()));

    ZabbixMetric metric3 = new ZabbixMetric(getTime(), testHost2, testGroup, itemName1, itemKey1,
            Double.toString(Math.random()));

    ZabbixMetric metric4 = new ZabbixMetric(getTime(), testHost2, testGroup, itemName2, itemKey2,
            Double.toString(Math.random()));

    metricList.add(metric1);
    metricList.add(metric2);
    metricList.add(metric3);
    metricList.add(metric4);

    checkResult = zabbixService.checkZabbixData(metricList);
  }

  private void deleteTestDataInZabbix() {
    ArrayList<String> hostList = new ArrayList<>();

    hostList.add(testHost1);
    hostList.add(testHost2);

    zabbixService.initZabbixApi();
    zabbixService.loginZabbix();
    ZabbixAPIResult deleteResult = zabbixService.getZabbixApi().hostListDeleteByName(hostList);
    //printAPIResult(deleteResult);
    zabbixService.destroyZabbixApi();
  }

  @Test
  public void testACheckZabbixData() {
    Map<String, ZabbixModel> dataMap = (Map)checkResult.getData();
    assertThat(dataMap, hasKey("testHost1"));
    assertThat(dataMap, hasKey("testHost2"));
    assertThat(dataMap.size(), comparesEqualTo(2));

    ZabbixModel host1 = dataMap.get("testHost1");
    ZabbixModel host2 = dataMap.get("testHost2");

    List<ZabbixMetric> items1List = host1.getItems();
    List<ZabbixMetric> items2List = host2.getItems();
    assertThat(items1List, hasSize(2));
    assertThat(items2List, hasSize(2));
  }

  private String getTime() {
    String pattern = "yyyy-MM-dd HH:mm:ss";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

    return simpleDateFormat.format(new Date());
  }

  @Test
  public void testBCreateHostAndItem() {
    Map<String, ZabbixModel> dataMap = (Map<String, ZabbixModel>)checkResult.getData();

    printAPIResult(checkResult);
    RestResult createResult = zabbixService.createHostItem(dataMap);
    printAPIResult(createResult);

    zabbixService.initZabbixApi();
    zabbixService.loginZabbix();
    assertThat(zabbixService.getZabbixApi().hostExists(testHost1), is(true));
    assertThat(zabbixService.getZabbixApi().hostExists(testHost2), is(true));

    checkItemsExist(testHost1);
    checkItemsExist(testHost2);
  }

  private void checkItemsExist(String host) {
    ArrayList<String> itemKeyList = new ArrayList();
    itemKeyList.add(itemKey1);
    itemKeyList.add(itemKey2);
    ArrayList<String> itemNameList = new ArrayList();
    itemNameList.add(itemName1);
    itemNameList.add(itemName2);


    ZabbixAPIResult itemsGetResult = zabbixService.getZabbixApi().itemGetByHostNameAndItemKey(host, itemKeyList);
    if (!itemsGetResult.isFail()) {
      JsonNode itemsArray = (JsonNode) itemsGetResult.getData();
      if (itemsArray.size() > 0) {
        itemsArray.forEach(item -> {
          String key = item.get("key_").asText();
          String name = item.get("name").asText();
          assertThat(itemKeyList.contains(key), is(true));
          assertThat(itemNameList.contains(name), is(true));
        });
      }
    }
  }

  private void printAPIResult(Object restResult) {
    try {
      System.out.println("Object is :" + new ObjectMapper().
              writerWithDefaultPrettyPrinter().writeValueAsString(restResult));
    } catch (Exception exception) {
      exception.printStackTrace();
    }
  }

  @Test
  public void testCSendToZabbix() throws InterruptedException {
    ArrayList<DataObject> dataList = new ArrayList<>();

    for (int i=0; i < 10; i++) {
      DataObject data = generateData();
      dataList.add(data);
      Thread.sleep(5000);
    }

    RestResult restResult = zabbixService.send(dataList);
    if (restResult.getCode() == ResultCode.SUCCESS.code()) {
      SenderResult senderResult = (SenderResult) restResult.getData();
      assertThat(senderResult.getTotal(), comparesEqualTo(10));
      assertThat(senderResult.getFailed(), comparesEqualTo(0));
    }

    deleteTestDataInZabbix();
  }

  private DataObject generateData() {
    long clock = System.currentTimeMillis()/1000;

    int hostNum = Math.random()>0.5?1:2;
    String host = "testHost" + hostNum;

    int itemNum = Math.random()>0.5?1:2;
    String itemKey = "itemKey" + itemNum;

    return new DataObject(clock, host, itemKey, Double.toString(Math.random()));
  }
}
