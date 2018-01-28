/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.example;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;

@Category({ ClientTests.class, MediumTests.class })
public class TestHttpProxyExample {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHttpProxyExample.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final String FAMILY = "cf";

  private static final String QUALIFIER = "cq";

  private static final String URL_TEMPLCATE = "http://localhost:%d/%s/%s/%s:%s";

  private static final String ROW = "row";

  private static final String VALUE = "value";

  private static HttpProxyExample PROXY;

  private static int PORT;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    UTIL.createTable(TABLE_NAME, Bytes.toBytes(FAMILY));
    PROXY = new HttpProxyExample(UTIL.getConfiguration(), 0);
    PROXY.start();
    PORT = PROXY.port();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (PROXY != null) {
      PROXY.stop();
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
      HttpPut put = new HttpPut(
          String.format(URL_TEMPLCATE, PORT, TABLE_NAME.getNameAsString(), ROW, FAMILY, QUALIFIER));
      put.setEntity(EntityBuilder.create().setText(VALUE)
          .setContentType(ContentType.create("text-plain", StandardCharsets.UTF_8)).build());
      try (CloseableHttpResponse resp = client.execute(put)) {
        assertEquals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
      }
      HttpGet get = new HttpGet(
          String.format(URL_TEMPLCATE, PORT, TABLE_NAME.getNameAsString(), ROW, FAMILY, QUALIFIER));
      try (CloseableHttpResponse resp = client.execute(get)) {
        assertEquals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
        assertEquals("value",
          Bytes.toString(ByteStreams.toByteArray(resp.getEntity().getContent())));
      }
      get = new HttpGet(String.format(URL_TEMPLCATE, PORT, TABLE_NAME.getNameAsString(), "whatever",
        FAMILY, QUALIFIER));
      try (CloseableHttpResponse resp = client.execute(get)) {
        assertEquals(HttpStatus.SC_NOT_FOUND, resp.getStatusLine().getStatusCode());
      }
    }
  }
}
