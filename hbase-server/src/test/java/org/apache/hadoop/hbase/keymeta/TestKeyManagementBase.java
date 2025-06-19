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
package org.apache.hadoop.hbase.keymeta;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestKeyManagementBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(
      TestKeyManagementBase.class);

  @Test
  public void testGetKeyProviderWithInvalidProvider() throws Exception {
    // Setup configuration with a non-ManagedKeyProvider
    Configuration conf = new Configuration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY,
        "org.apache.hadoop.hbase.keymeta.DummyKeyProvider");

    Server mockServer = mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(conf);

    KeyManagementBase keyMgmt = new TestKeyManagement(mockServer);

    // Should throw RuntimeException when provider is not ManagedKeyProvider
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      keyMgmt.getKeyProvider();
    });

    assertTrue(exception.getMessage().contains("expected to be of type ManagedKeyProvider"));
  }

  private static class TestKeyManagement extends KeyManagementBase {
    public TestKeyManagement(Server server) {
      super(server);
    }
  }
}