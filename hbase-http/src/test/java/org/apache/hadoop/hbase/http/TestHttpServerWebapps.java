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
package org.apache.hadoop.hbase.http;

import java.io.FileNotFoundException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test webapp loading
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestHttpServerWebapps extends HttpServerFunctionalTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHttpServerWebapps.class);

  private static final Logger log = LoggerFactory.getLogger(TestHttpServerWebapps.class);

  /**
   * Test that the test server is loadable on the classpath
   * @throws Throwable if something went wrong
   */
  @Test
  public void testValidServerResource() throws Throwable {
    HttpServer server = null;
    try {
      server = createServer("test");
    } finally {
      stop(server);
    }
  }

  /**
   * Test that an invalid webapp triggers an exception
   * @throws Throwable if something went wrong
   */
  @Test
  public void testMissingServerResource() throws Throwable {
    try {
      HttpServer server = createServer("NoSuchWebapp");
      // should not have got here.
      // close the server
      String serverDescription = server.toString();
      stop(server);
      fail("Expected an exception, got " + serverDescription);
    } catch (FileNotFoundException expected) {
      log.debug("Expected exception " + expected, expected);
    }
  }

}
