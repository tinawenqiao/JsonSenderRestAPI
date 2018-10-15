package org.cmbc.bigdata.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.cmbc.bigdata.model.RestResult;
import org.cmbc.bigdata.model.ResultCode;
import org.cmbc.bigdata.utils.ElasticsearchUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Log4j
@Getter
@Setter
@Service
public class ElasticsearchService {
  @Autowired
  private ElasticsearchUtils elasticsearchUtils;

  private RestClient restClient;

  @PostConstruct
  public void init() {
    restClient = elasticsearchUtils.getRestClient();
  }

  public RestResult write(String index, String type, Map doc) {
    String method = "post";
    String endpoint = String.format("/%s/%s", index, type);
    String jsonStr = "";
    RestResult restResult;

    log.info("index:" + index + ",type:" + type + ", doc:" + doc.toString());
    ObjectMapper mapper = new ObjectMapper();
    try {
      jsonStr = mapper.writeValueAsString(doc);
    } catch (JsonProcessingException jsonProcessingException) {
      restResult = RestResult.failure(ResultCode.PARAM_IS_INVALID,
              ResultCode.PARAM_IS_INVALID.message(), jsonProcessingException.getMessage());
      restResult.setStatus(HttpStatus.BAD_REQUEST);
    }

    NStringEntity requestBody = new NStringEntity(jsonStr, ContentType.APPLICATION_JSON);

    return performOnElasticsearch(method, endpoint, Collections.singletonMap("pretty", "true"), requestBody);
  }

  public RestResult searchByQueryParams(String index, String queryStr) {
    String method = "get";
    String endpoint = String.format("/%s/_search", index);
    RestResult restResult;

    log.info("search index:" + index + ", queryStr:" + queryStr);

    Map<String, String> paramMap = new HashMap<String, String>();
    paramMap.put("q", queryStr);
    paramMap.put("pretty", "true");


    return performOnElasticsearch(method, endpoint, paramMap, null);
  }

  public RestResult searchByQueryDSL(String index, String queryDSL) {
    String method = "get";
    String endpoint = String.format("/%s/_search", index);

    NStringEntity requestBody = new NStringEntity(queryDSL, ContentType.APPLICATION_JSON);

    return performOnElasticsearch(method, endpoint, Collections.singletonMap("pretty", "true"), requestBody);
  }

  public RestResult deleteByQuery(String index, String queryDSL) {
    String method = "post";
    String endpoint = String.format("/%s/_delete_by_query", index);

    NStringEntity requestBody = new NStringEntity(queryDSL, ContentType.APPLICATION_JSON);

    return performOnElasticsearch(method, endpoint, Collections.singletonMap("pretty", "true"), requestBody);
  }

  public boolean docExists(String index, String queryMethod, String queryStr) {
    RestResult restResult = new RestResult();

    if (queryMethod.equals("queryParam")) {
      restResult = searchByQueryParams(index, queryStr);
    } else if (queryMethod.equals("queryDSL")) {
      restResult = searchByQueryDSL(index, queryStr);
    }
    boolean exists = false;

    if (restResult.getCode() == ResultCode.SUCCESS.code()) {
      try {
        JsonNode data = new ObjectMapper().readTree(restResult.getData().toString());
        int hits = data.get("hits").get("total").asInt();
        if (hits > 0) return true;
      } catch (IOException ioException) {
        return exists;
      }
    }

    return exists;
  }

  private RestResult performOnElasticsearch(String method, String endpoint, Map params, HttpEntity entity) {
    RestResult restResult;

    if (restClient != null) {
      try {
        Response writeResponse = restClient.performRequest(method, endpoint, params, entity);
        String esResponse = EntityUtils.toString(writeResponse.getEntity());
        log.info("Response from Elasticsearch is :" + esResponse);
        //System.out.println(("Response from Elasticsearch is :" + esResponse));
        restResult = RestResult.success(esResponse, ResultCode.SUCCESS.message());
      } catch (IOException ioException) {
        log.error("Error happens when performing request on Elasticsearch. Exception is :" + ioException);
        restResult = RestResult.failure(ResultCode.INTERFACE_ELASTICSEARCH_PERFORM_ERROR,
                ResultCode.INTERFACE_ELASTICSEARCH_PERFORM_ERROR.message(), ioException.getMessage());
        restResult.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
      }
    } else {
      log.info("RestClient is null");
      restResult = RestResult.failure(ResultCode.INTERFACE_ELASTICSEARCH_CLIENT_GET_ERROR,
              ResultCode.INTERFACE_ELASTICSEARCH_CLIENT_GET_ERROR.message(),
              ResultCode.INTERFACE_ELASTICSEARCH_CLIENT_GET_ERROR.message());
      restResult.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    return restResult;
  }
}
