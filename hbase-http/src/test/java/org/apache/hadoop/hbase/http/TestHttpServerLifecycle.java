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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestHttpServerLifecycle extends HttpServerFunctionalTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHttpServerLifecycle.class);

  /**
   * Check that a server is alive by probing the {@link HttpServer#isAlive()} method
   * and the text of its toString() description
   * @param server server
   */
  private void assertAlive(HttpServer server) {
    assertTrue("Server is not alive", server.isAlive());
    assertToStringContains(server, HttpServer.STATE_DESCRIPTION_ALIVE);
  }

  private void assertNotLive(HttpServer server) {
    assertTrue("Server should not be live", !server.isAlive());
    assertToStringContains(server, HttpServer.STATE_DESCRIPTION_NOT_LIVE);
  }

  /**
   * Test that the server is alive once started
   *
   * @throws Throwable on failure
   */
  @Ignore ("Hangs on occasion; see HBASE-14430") @Test
  public void testCreatedServerIsNotAlive() throws Throwable {
    HttpServer server = createTestServer();
    assertNotLive(server);
  }

  @Ignore ("Hangs on occasion; see HBASE-14430") @Test
  public void testStopUnstartedServer() throws Throwable {
    HttpServer server = createTestServer();
    stop(server);
  }

  /**
   * Test that the server is alive once started
   *
   * @throws Throwable on failure
   */
  @Ignore ("Hangs on occasion; see HBASE-14430") @Test
  public void testStartedServerIsAlive() throws Throwable {
    HttpServer server = null;
    server = createTestServer();
    assertNotLive(server);
    server.start();
    assertAlive(server);
    stop(server);
  }

  /**
   * Assert that the result of {@link HttpServer#toString()} contains the specific text
   * @param server server to examine
   * @param text text to search for
   */
  private void assertToStringContains(HttpServer server, String text) {
    String description = server.toString();
    assertTrue("Did not find \"" + text + "\" in \"" + description + "\"",
               description.contains(text));
  }

  /**
   * Test that the server is not alive once stopped
   *
   * @throws Throwable on failure
   */
  @Ignore ("Hangs on occasion; see HBASE-14430") @Test
  public void testStoppedServerIsNotAlive() throws Throwable {
    HttpServer server = createAndStartTestServer();
    assertAlive(server);
    stop(server);
    assertNotLive(server);
  }

  /**
   * Test that the server is not alive once stopped
   *
   * @throws Throwable on failure
   */
  @Ignore ("Hangs on occasion; see HBASE-14430") @Test
  public void testStoppingTwiceServerIsAllowed() throws Throwable {
    HttpServer server = createAndStartTestServer();
    assertAlive(server);
    stop(server);
    assertNotLive(server);
    stop(server);
    assertNotLive(server);
  }

  /**
   * Test that the server is alive once started
   *
   * @throws Throwable
   *           on failure
   */
  @Ignore ("Hangs on occasion; see HBASE-14430") @Test
  public void testWepAppContextAfterServerStop() throws Throwable {
    HttpServer server = null;
    String key = "test.attribute.key";
    String value = "test.attribute.value";
    server = createTestServer();
    assertNotLive(server);
    server.start();
    server.setAttribute(key, value);
    assertAlive(server);
    assertEquals(value, server.getAttribute(key));
    stop(server);
    assertNull("Server context should have cleared", server.getAttribute(key));
  }
}
