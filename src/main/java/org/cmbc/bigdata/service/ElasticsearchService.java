package org.cmbc.bigdata.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
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

    System.out.println("index:" + index + ",type:" + type + ", doc:" + doc.toString());
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

    log.info("RestClient is null : " + (restClient==null));
    if (restClient != null) {
      try {
        Response writeResponse = restClient.performRequest(method, endpoint, Collections.emptyMap(),
                requestBody);
        String esResponse = EntityUtils.toString(writeResponse.getEntity());
        restResult = RestResult.success(esResponse, ResultCode.SUCCESS.message());
      } catch (IOException ioException) {
        restResult = RestResult.failure(ResultCode.INTERFACE_ELASTICSEARCH_PERFORM_ERROR,
                ResultCode.INTERFACE_ELASTICSEARCH_PERFORM_ERROR.message(), ioException.getMessage());
        restResult.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
      }
    } else {
      restResult = RestResult.failure(ResultCode.INTERFACE_ELASTICSEARCH_CLIENT_GET_ERROR,
              ResultCode.INTERFACE_ELASTICSEARCH_CLIENT_GET_ERROR.message(),
              ResultCode.INTERFACE_ELASTICSEARCH_CLIENT_GET_ERROR.message());
      restResult.setStatus(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    return restResult;
  }
}
