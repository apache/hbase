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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestHttpRequestLog {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHttpRequestLog.class);

  @Test
  public void testAppenderUndefined() {
    RequestLog requestLog = HttpRequestLog.getRequestLog("test");
    assertNull("RequestLog should be null", requestLog);
  }

  @Test
  public void testAppenderDefined() {
    HttpRequestLogAppender requestLogAppender = new HttpRequestLogAppender();
    requestLogAppender.setName("testrequestlog");
    Logger.getLogger("http.requests.test").addAppender(requestLogAppender);
    RequestLog requestLog = HttpRequestLog.getRequestLog("test");
    Logger.getLogger("http.requests.test").removeAppender(requestLogAppender);
    assertNotNull("RequestLog should not be null", requestLog);
    assertEquals("Class mismatch", NCSARequestLog.class, requestLog.getClass());
  }
}
