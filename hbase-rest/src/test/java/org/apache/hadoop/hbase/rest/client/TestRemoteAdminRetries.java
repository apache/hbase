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
package org.apache.hadoop.hbase.rest.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests {@link RemoteAdmin} retries.
 */
@Category({ RestTests.class, SmallTests.class })
public class TestRemoteAdminRetries {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRemoteAdminRetries.class);

  private static final int SLEEP_TIME = 50;
  private static final int RETRIES = 3;
  private static final long MAX_TIME = SLEEP_TIME * (RETRIES - 1);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private RemoteAdmin remoteAdmin;
  private Client client;

  @Before
  public void setup() throws Exception {
    client = mock(Client.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine())
      .thenReturn(new BasicStatusLine(new ProtocolVersion("Http", 1, 0), 509, "test"));

    when(client.execute(any(), any())).thenReturn(response);
    Configuration configuration = TEST_UTIL.getConfiguration();

    configuration.setInt("hbase.rest.client.max.retries", RETRIES);
    configuration.setInt("hbase.rest.client.sleep", SLEEP_TIME);

    remoteAdmin = new RemoteAdmin(client, TEST_UTIL.getConfiguration(), "MyTable");
  }

  @Test
  public void testFailingGetRestVersion() throws Exception {
    testTimedOutGetCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteAdmin.getRestVersion();
      }
    });
  }

  @Test
  public void testFailingGetClusterStatus() throws Exception {
    testTimedOutGetCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteAdmin.getClusterStatus();
      }
    });
  }

  @Test
  public void testFailingGetClusterVersion() throws Exception {
    testTimedOutGetCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteAdmin.getClusterVersion();
      }
    });
  }

  @Test
  public void testFailingGetTableAvailable() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteAdmin.isTableAvailable(Bytes.toBytes("TestTable"));
      }
    });
  }

  @Test
  public void testFailingCreateTable() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteAdmin
          .createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf("TestTable")).build());
      }
    });
    verify(client, times(RETRIES)).execute(any(), any());
  }

  @Test
  public void testFailingDeleteTable() throws Exception {
    testTimedOutCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteAdmin.deleteTable("TestTable");
      }
    });
    verify(client, times(RETRIES)).execute(any(), any());
  }

  @Test
  public void testFailingGetTableList() throws Exception {
    testTimedOutGetCall(new CallExecutor() {
      @Override
      public void run() throws Exception {
        remoteAdmin.getTableList();
      }
    });
  }

  private void testTimedOutGetCall(CallExecutor callExecutor) throws Exception {
    testTimedOutCall(callExecutor);
    verify(client, times(RETRIES)).execute(any(), any());
  }

  private void testTimedOutCall(CallExecutor callExecutor) throws Exception {
    long start = EnvironmentEdgeManager.currentTime();
    try {
      callExecutor.run();
      fail("should be timeout exception!");
    } catch (IOException e) {
      assertTrue(Pattern.matches(".*MyTable.*timed out", e.toString()));
    }
    assertTrue((EnvironmentEdgeManager.currentTime() - start) > MAX_TIME);
  }

  private static interface CallExecutor {
    void run() throws Exception;
  }

}
