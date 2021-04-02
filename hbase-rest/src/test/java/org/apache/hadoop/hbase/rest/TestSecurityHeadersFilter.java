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
package org.apache.hadoop.hbase.rest;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RestTests.class, MediumTests.class})
public class TestSecurityHeadersFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSecurityHeadersFilter.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
      new HBaseRESTTestingUtility();
  private static Client client;

  @After
  public void tearDown() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testDefaultValues() throws Exception {
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost",
        REST_TEST_UTIL.getServletPort()));

    String path = "/version/cluster";
    Response response = client.get(path);
    assertThat(response.getCode(), equalTo(200));

    assertThat("Header 'X-Content-Type-Options' is missing from Rest response",
        response.getHeader("X-Content-Type-Options"), is(not((String)null)));
    assertThat("Header 'X-Content-Type-Options' has invalid default value",
        response.getHeader("X-Content-Type-Options"), equalTo("nosniff"));

    assertThat("Header 'X-XSS-Protection' is missing from Rest response",
        response.getHeader("X-XSS-Protection"), is(not((String)null)));
    assertThat("Header 'X-XSS-Protection' has invalid default value",
        response.getHeader("X-XSS-Protection"), equalTo("1; mode=block"));

    assertThat("Header 'Strict-Transport-Security' should be missing from Rest response," +
            "but it's present",
        response.getHeader("Strict-Transport-Security"), is((String)null));
    assertThat("Header 'Content-Security-Policy' should be missing from Rest response," +
            "but it's present",
        response.getHeader("Content-Security-Policy"), is((String)null));
  }

  @Test
  public void testHstsAndCspSettings() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.http.filter.hsts.value",
        "max-age=63072000;includeSubDomains;preload");
    TEST_UTIL.getConfiguration().set("hbase.http.filter.csp.value",
        "default-src https: data: 'unsafe-inline' 'unsafe-eval'");
    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());
    client = new Client(new Cluster().add("localhost",
        REST_TEST_UTIL.getServletPort()));

    String path = "/version/cluster";
    Response response = client.get(path);
    assertThat(response.getCode(), equalTo(200));

    assertThat("Header 'Strict-Transport-Security' is missing from Rest response",
        response.getHeader("Strict-Transport-Security"), is(not((String)null)));
    assertThat("Header 'Strict-Transport-Security' has invalid value",
        response.getHeader("Strict-Transport-Security"),
        equalTo("max-age=63072000;includeSubDomains;preload"));

    assertThat("Header 'Content-Security-Policy' is missing from Rest response",
        response.getHeader("Content-Security-Policy"), is(not((String)null)));
    assertThat("Header 'Content-Security-Policy' has invalid value",
        response.getHeader("Content-Security-Policy"),
        equalTo("default-src https: data: 'unsafe-inline' 'unsafe-eval'"));
  }
}
