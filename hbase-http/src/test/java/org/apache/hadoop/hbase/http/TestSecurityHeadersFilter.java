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
package org.apache.hadoop.hbase.http;

import static org.apache.hadoop.hbase.http.HttpServerFunctionalTest.createTestServer;
import static org.apache.hadoop.hbase.http.HttpServerFunctionalTest.getServerURL;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({HttpServerFunctionalTest.class, MediumTests.class})
public class TestSecurityHeadersFilter {
  private static URL baseUrl;
  private HttpServer http;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSecurityHeadersFilter.class);

  @After
  public void tearDown() throws Exception {
    http.stop();
  }

  @Test
  public void testDefaultValues() throws Exception {
    http = createTestServer();
    http.start();
    baseUrl = getServerURL(http);

    URL url = new URL(baseUrl, "/");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertThat(conn.getResponseCode(), equalTo(HttpURLConnection.HTTP_OK));

    assertThat("Header 'X-Content-Type-Options' is missing",
        conn.getHeaderField("X-Content-Type-Options"), is(not((String)null)));
    assertThat(conn.getHeaderField("X-Content-Type-Options"), equalTo("nosniff"));
    assertThat("Header 'X-XSS-Protection' is missing",
        conn.getHeaderField("X-XSS-Protection"), is(not((String)null)));
    assertThat("Header 'X-XSS-Protection' has invalid value",
        conn.getHeaderField("X-XSS-Protection"), equalTo("1; mode=block"));

    assertThat("Header 'Strict-Transport-Security' should be missing from response," +
            "but it's present",
        conn.getHeaderField("Strict-Transport-Security"), is((String)null));
    assertThat("Header 'Content-Security-Policy' should be missing from response," +
            "but it's present",
        conn.getHeaderField("Content-Security-Policy"), is((String)null));
  }

  @Test
  public void testHstsAndCspSettings() throws IOException {
    Configuration conf = new Configuration();
    conf.set("hbase.http.filter.hsts.value",
        "max-age=63072000;includeSubDomains;preload");
    conf.set("hbase.http.filter.csp.value",
        "default-src https: data: 'unsafe-inline' 'unsafe-eval'");
    http = createTestServer(conf);
    http.start();
    baseUrl = getServerURL(http);

    URL url = new URL(baseUrl, "/");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertThat(conn.getResponseCode(), equalTo(HttpURLConnection.HTTP_OK));

    assertThat("Header 'Strict-Transport-Security' is missing from Rest response",
        conn.getHeaderField("Strict-Transport-Security"), Is.is(not((String)null)));
    assertThat("Header 'Strict-Transport-Security' has invalid value",
        conn.getHeaderField("Strict-Transport-Security"),
        IsEqual.equalTo("max-age=63072000;includeSubDomains;preload"));

    assertThat("Header 'Content-Security-Policy' is missing from Rest response",
        conn.getHeaderField("Content-Security-Policy"), Is.is(not((String)null)));
    assertThat("Header 'Content-Security-Policy' has invalid value",
        conn.getHeaderField("Content-Security-Policy"),
        IsEqual.equalTo("default-src https: data: 'unsafe-inline' 'unsafe-eval'"));
  }
}
