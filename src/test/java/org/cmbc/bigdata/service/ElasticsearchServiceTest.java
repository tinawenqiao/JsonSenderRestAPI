package org.cmbc.bigdata.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class ElasticsearchServiceTest {
  @Autowired
  private ElasticsearchService elasticsearchService;

  private static final String index = "test";
  private static final String fieldName = "name";
  private static final String fieldValue = "alongvalue2";

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    String queryDSL = "{\n" +
            "    \"query\" : {\n" +
            "    \"match\": { \"" + fieldName + "\":\"" + fieldValue + "\"} \n" +
            "} \n"+
            "}";
    elasticsearchService.deleteByQuery(index, queryDSL);
  }

  @Test
  public void testEsWrite() throws InterruptedException {
    String type = "type";
    Map<String, String> doc = new HashMap<>();
    doc.put(fieldName, fieldValue);
    elasticsearchService.write(index, type, doc);

    Thread.sleep(5000);
    /*
    //Query by params
    String queryParam = fieldName + ":" + fieldValue;
    String queryMethod = "queryParam";
    boolean exists = elasticsearchService.docExists(index, queryMethod, queryParam);
    */

    //Query by DSL
    String queryMethod = "queryDSL";

    String queryStr = "{\n" +
        "    \"query\" : {\n" +
        "    \"match\": { \"" + fieldName + "\":\"" + fieldValue + "\"} \n" +
        "} \n"+
        "}";
    boolean exists = elasticsearchService.docExists(index, queryMethod, queryStr);

    assertThat(exists, is(true));
  }
}