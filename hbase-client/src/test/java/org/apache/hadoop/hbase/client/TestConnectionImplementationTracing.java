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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestConnectionImplementationTracing extends TestTracingBase {

  ConnectionImplementation conn;

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    conn = new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent(),
      Collections.emptyMap());
  }

  @AfterEach
  public void tearDown() throws IOException {
    Closeables.close(conn, true);
  }

  @Test
  public void testHbck() throws IOException {
    conn.getHbck();
    assertTrace(ConnectionImplementation.class.getSimpleName(), "getHbck", MASTER_HOST, null);
  }

  @Test
  public void testHbckWithServerName() throws IOException {
    ServerName otherHost = ServerName.valueOf("localhost2", 16010, System.currentTimeMillis());
    conn.getHbck(otherHost);
    assertTrace(ConnectionImplementation.class.getSimpleName(), "getHbck", otherHost, null);
  }

  @Test
  public void testClose() throws IOException {
    conn.close();
    assertTrace(ConnectionImplementation.class.getSimpleName(), "close", null, null);
  }

}
