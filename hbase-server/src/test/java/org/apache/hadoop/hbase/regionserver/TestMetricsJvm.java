/*
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
package org.apache.hadoop.hbase.regionserver;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MetricsTests.class, SmallTests.class })
public class TestMetricsJvm {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsJvm.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @BeforeClass
  public static void before() throws Exception {
    conf = UTIL.getConfiguration();
    // The master info server does not run in tests by default.
    // Set it to ephemeral port so that it will start
    conf.setInt(HConstants.MASTER_INFO_PORT, 0);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void after() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testJvmMetrics() throws Exception {
    final Pair<Integer, String> jmxPage = getJmxPage("?qry=Hadoop:service=HBase,name=JvmMetrics*");
    assertNotNull(jmxPage);

    final Integer responseCode = jmxPage.getFirst();
    final String responseBody = jmxPage.getSecond();

    assertEquals(HttpURLConnection.HTTP_OK, responseCode.intValue());
    assertNotNull(responseBody);

    assertNotFind("\"tag.ProcessName\"\\s*:\\s*\"IO\"", responseBody);
    assertReFind("\"tag.ProcessName\"\\s*:\\s*\"Master\"", responseBody);
  }

  private Pair<Integer, String> getJmxPage(String query) throws Exception {
    URL url = new URL("http://localhost:"
      + UTIL.getHBaseCluster().getMaster().getInfoServer().getPort() + "/jmx" + query);
    return getUrlContent(url);
  }

  private Pair<Integer, String> getUrlContent(URL url) throws Exception {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      CloseableHttpResponse resp = client.execute(new HttpGet(url.toURI()));
      int code = resp.getStatusLine().getStatusCode();
      if (code == HttpURLConnection.HTTP_OK) {
        return new Pair<>(code, EntityUtils.toString(resp.getEntity()));
      }
      return new Pair<>(code, null);
    }
  }

  private void assertReFind(String re, String value) {
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(value);
    assertTrue("'" + p + "' does not match " + value, m.find());
  }

  private void assertNotFind(String re, String value) {
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(value);
    assertFalse("'" + p + "' should not match " + value, m.find());
  }
}
