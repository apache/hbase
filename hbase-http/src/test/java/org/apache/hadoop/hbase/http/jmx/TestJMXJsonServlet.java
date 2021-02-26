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
package org.apache.hadoop.hbase.http.jmx;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.http.HttpServerFunctionalTest;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestJMXJsonServlet extends HttpServerFunctionalTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestJMXJsonServlet.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestJMXJsonServlet.class);
  private static HttpServer server;
  private static URL baseUrl;

  @BeforeClass public static void setup() throws Exception {
    // Eclipse doesn't pick this up correctly from the plugin
    // configuration in the pom.
    System.setProperty(HttpServerFunctionalTest.TEST_BUILD_WEBAPPS, "target/test-classes/webapps");
    server = createTestServer();
    server.start();
    baseUrl = getServerURL(server);
  }

  @AfterClass public static void cleanup() throws Exception {
    server.stop();
  }

  public static void assertReFind(String re, String value) {
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(value);
    assertTrue("'"+p+"' does not match "+value, m.find());
  }

  public static void assertNotFind(String re, String value) {
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(value);
    assertFalse("'"+p+"' should not match "+value, m.find());
  }

  @Test public void testQuery() throws Exception {
    String result = readOutput(new URL(baseUrl, "/jmx?qry=java.lang:type=Runtime"));
    LOG.info("/jmx?qry=java.lang:type=Runtime RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Runtime\"", result);
    assertReFind("\"modelerType\"", result);

    result = readOutput(new URL(baseUrl, "/jmx?qry=java.lang:type=Memory"));
    LOG.info("/jmx?qry=java.lang:type=Memory RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"modelerType\"", result);

    result = readOutput(new URL(baseUrl, "/jmx"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);

    // test to get an attribute of a mbean
    result = readOutput(new URL(baseUrl,
        "/jmx?get=java.lang:type=Memory::HeapMemoryUsage"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"committed\"\\s*:", result);

    // negative test to get an attribute of a mbean
    result = readOutput(new URL(baseUrl,
        "/jmx?get=java.lang:type=Memory::"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("\"ERROR\"", result);

    // test to get JSONP result
    result = readOutput(new URL(baseUrl, "/jmx?qry=java.lang:type=Memory&callback=mycallback1"));
    LOG.info("/jmx?qry=java.lang:type=Memory&callback=mycallback RESULT: "+result);
    assertReFind("^mycallback1\\(\\{", result);
    assertReFind("\\}\\);$", result);

    // negative test to get an attribute of a mbean as JSONP
    result = readOutput(new URL(baseUrl,
        "/jmx?get=java.lang:type=Memory::&callback=mycallback2"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("^mycallback2\\(\\{", result);
    assertReFind("\"ERROR\"", result);
    assertReFind("\\}\\);$", result);

    // test to get an attribute of a mbean as JSONP
    result = readOutput(new URL(baseUrl,
        "/jmx?get=java.lang:type=Memory::HeapMemoryUsage&callback=mycallback3"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("^mycallback3\\(\\{", result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"committed\"\\s*:", result);
    assertReFind("\\}\\);$", result);
  }

  @Test
  public void testGetPattern() throws Exception {
    // test to get an attribute of a mbean as JSONP
    String result = readOutput(
      new URL(baseUrl, "/jmx?get=java.lang:type=Memory::[a-zA-z_]*NonHeapMemoryUsage"));
    LOG.info("/jmx RESULT: " + result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"committed\"\\s*:", result);
    assertReFind("\"NonHeapMemoryUsage\"\\s*:", result);
    assertNotFind("\"HeapMemoryUsage\"\\s*:", result);

    result =
        readOutput(new URL(baseUrl, "/jmx?get=java.lang:type=Memory::[^Non]*HeapMemoryUsage"));
    LOG.info("/jmx RESULT: " + result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"committed\"\\s*:", result);
    assertReFind("\"HeapMemoryUsage\"\\s*:", result);
    assertNotFind("\"NonHeapHeapMemoryUsage\"\\s*:", result);

    result = readOutput(new URL(baseUrl,
        "/jmx?get=java.lang:type=Memory::[a-zA-z_]*HeapMemoryUsage,[a-zA-z_]*NonHeapMemoryUsage"));
    LOG.info("/jmx RESULT: " + result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"committed\"\\s*:", result);
  }

  @Test
  public void testPatternMatching() throws Exception {
    assertReFind("[a-zA-z_]*Table1[a-zA-z_]*memStoreSize",
      "Namespace_default_table_Table1_metric_memStoreSize");
    assertReFind("[a-zA-z_]*memStoreSize", "Namespace_default_table_Table1_metric_memStoreSize");
  }

  @Test
  public void testDisallowedJSONPCallback() throws Exception {
    String callback = "function(){alert('bigproblems!')};foo";
    URL url = new URL(
        baseUrl, "/jmx?qry=java.lang:type=Memory&callback="+URLEncoder.encode(callback, "UTF-8"));
    HttpURLConnection cnxn = (HttpURLConnection) url.openConnection();
    assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, cnxn.getResponseCode());
  }

  @Test
  public void testUnderscoresInJSONPCallback() throws Exception {
    String callback = "my_function";
    URL url = new URL(
        baseUrl, "/jmx?qry=java.lang:type=Memory&callback="+URLEncoder.encode(callback, "UTF-8"));
    HttpURLConnection cnxn = (HttpURLConnection) url.openConnection();
    assertEquals(HttpServletResponse.SC_OK, cnxn.getResponseCode());
  }
}
