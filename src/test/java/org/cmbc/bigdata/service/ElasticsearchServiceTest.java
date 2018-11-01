package org.cmbc.bigdata.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class ElasticsearchServiceTest {
  @Autowired
  private ElasticsearchService elasticsearchService;

  private static final String index = "test";
  private static final String fieldName = "field";
  private static final String fieldValue = "testvalue1";
  private static final String bulkFieldName = "bulkfield";
  private static final String bulkFieldValue1 = "testvalue2";
  private static final String bulkFieldValue2 = "testvalue3";

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

    queryDSL = "{\n" +
            "    \"query\" : {\n" +
            "    \"match\": { \"" + bulkFieldName + "\":\"" + bulkFieldValue1 + "\"} \n" +
            "} \n"+
            "}";
    elasticsearchService.deleteByQuery(index, queryDSL);

    queryDSL = "{\n" +
            "    \"query\" : {\n" +
            "    \"match\": { \"" + bulkFieldName + "\":\"" + bulkFieldValue2 + "\"} \n" +
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

  @Test
  public void testEsBulkWrite() throws InterruptedException {
    String type = "type";

    Map<String, String> doc1 = new HashMap<>();
    doc1.put(bulkFieldName, bulkFieldValue1);

    Map<String, String> doc2 = new HashMap<>();
    doc2.put(bulkFieldName, bulkFieldValue2);

    List<Map> docs = new ArrayList();
    docs.add(doc1);
    docs.add(doc2);

    elasticsearchService.bulkWrite(index, type, docs);

    Thread.sleep(5000);
    /*
    //Query by params
    String queryParam = fieldName + ":" + fieldValue;
    String queryMethod = "queryParam";
    boolean exists = elasticsearchService.docExists(index, queryMethod, queryParam);
    */

    //Query by DSL
    String queryMethod = "queryDSL";

    String queryStr1 = "{\n" +
            "    \"query\" : {\n" +
            "    \"match\": { \"" + bulkFieldName + "\":\"" + bulkFieldValue1 + "\"} \n" +
            "} \n"+
            "}";
    boolean exists1 = elasticsearchService.docExists(index, queryMethod, queryStr1);

    String queryStr2 = "{\n" +
            "    \"query\" : {\n" +
            "    \"match\": { \"" + bulkFieldName + "\":\"" + bulkFieldValue2 + "\"} \n" +
            "} \n"+
            "}";
    boolean exists2 = elasticsearchService.docExists(index, queryMethod, queryStr2);

    assertThat(exists2, is(true));
  }
}