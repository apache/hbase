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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.hadoop.hbase.LocalHBaseCluster;

public class TestServerHttpUtils {
  private static final String PLAIN_TEXT = "text/plain";

  private TestServerHttpUtils() {
  }

  public static String getMasterInfoServerHostAndPort(LocalHBaseCluster cluster) {
    return "http://localhost:" + cluster.getActiveMaster().getInfoServer().getPort();
  }

  public static String getPageContent(URL url, String mimeType) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();

    assertEquals(200, conn.getResponseCode());
    assertEquals(mimeType, conn.getContentType());

    return TestServerHttpUtils.getResponseBody(conn);
  }

  public static String getMasterPageContent(LocalHBaseCluster cluster) throws IOException {
    URL debugDumpUrl = new URL(getMasterInfoServerHostAndPort(cluster) + "/dump");
    return getPageContent(debugDumpUrl, PLAIN_TEXT);
  }

  public static String getRegionServerPageContent(String hostName, int port) throws IOException {
    URL debugDumpUrl = new URL("http://" + hostName + ":" + port + "/dump");
    return getPageContent(debugDumpUrl, PLAIN_TEXT);
  }

  private static String getResponseBody(HttpURLConnection conn) throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String output;
    while ((output = br.readLine()) != null) {
      sb.append(output);
    }
    return sb.toString();
  }
}
