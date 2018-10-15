package org.cmbc.bigdata.model;

import java.util.ArrayList;
import java.util.List;

public enum ResultCode {
  /* 成功状态码 */
  SUCCESS(0, "Success"),

  /* 参数错误：10001-19999 */
  PARAM_IS_INVALID(10001, "Param is invalid."),

  /* Interface Elasticsearch Error：60001-69999 */
  INTERFACE_ELASTICSEARCH_CLIENT_GET_ERROR(60001, "RestClient got from elasticsearch is null."),
  INTERFACE_ELASTICSEARCH_PERFORM_ERROR(60002, "Perform Request on elasticsearch failure."),

  /* Interface Zabbix Error：70001-79999 */
  INTERFACE_ZABBIX_LOGIN_ERROR(70001,"Zabbix login failure."),
  INTERFACE_ZABBIX_SENDER_ERROR(70003,"Zabbix sender failure."),
  INTERFACE_ZABBIX_SENDER_PARTIAL_FAILURE(70004,"Zabbix sender partial failure."),
  INTERFACE_ZABBIX_SENDER_ALL_FAILURE(70005,"Zabbix sender all failure."),
  INTERFACE_ZABBIX_CREATE_HOSTGROUP_FAILURE(70010,"Zabbix host group create failure."),
  INTERFACE_ZABBIX_SEARCH_HOSTGROUP_FAILURE(70011,"Zabbix host group search failure."),
  INTERFACE_ZABBIX_CREATE_HOST_FAILURE(70012,"Zabbix host create failure."),
  INTERFACE_ZABBIX_SEARCH_HOST_FAILURE(70013,"Zabbix host search failure."),
  INTERFACE_ZABBIX_SEARCH_INTERFACE_FAILURE(70014,"Zabbix interface search failure."),
  INTERFACE_ZABBIX_CREATE_ITEM_FAILURE(70015,"Zabbix item create failure.");

  private Integer code;

  private String message;

  ResultCode(Integer code, String message) {
    this.code = code;
    this.message = message;
  }

  public Integer code() {
    return this.code;
  }

  public String message() {
    return this.message;
  }

  public static String getMessage(String name) {
    for (ResultCode item : ResultCode.values()) {
      if (item.name().equals(name)) {
        return item.message;
      }
    }
    return name;
  }

  public static Integer getCode(String name) {
    for (ResultCode item : ResultCode.values()) {
      if (item.name().equals(name)) {
        return item.code;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return this.name();
  }

  //校验重复的code值
  public static void main(String[] args) {
    System.out.println("success:" + ResultCode.SUCCESS);
    ResultCode[] ApiResultCodes = ResultCode.values();
    List<Integer> codeList = new ArrayList<Integer>();
    for (ResultCode ApiResultCode : ApiResultCodes) {
      if (codeList.contains(ApiResultCode.code)) {
        System.out.println(ApiResultCode.code);
      } else {
        codeList.add(ApiResultCode.code());
        System.out.println(ApiResultCode.code());
      }
    }
  }
}
