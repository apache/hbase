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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

@Category({ ClientTests.class, SmallTests.class })
public class TestConnectionFactoryApplyURIQueries {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConnectionFactoryApplyURIQueries.class);

  private Configuration conf;

  private MockedStatic<ConnectionRegistryFactory> mockedConnectionRegistryFactory;

  private ConnectionRegistry registry;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    mockedConnectionRegistryFactory = mockStatic(ConnectionRegistryFactory.class);
    registry = mock(ConnectionRegistry.class);
    mockedConnectionRegistryFactory
      .when(() -> ConnectionRegistryFactory.create(any(), any(), any())).thenReturn(registry);
    when(registry.getClusterId()).thenReturn(CompletableFuture.completedFuture("cluster"));
  }

  @After
  public void tearDown() {
    mockedConnectionRegistryFactory.closeOnDemand();
  }

  @Test
  public void testApplyURIQueries() throws Exception {
    ConnectionFactory.createConnection(new URI("hbase+rpc://server:16010?a=1&b=2&c"), conf);
    ArgumentCaptor<Configuration> captor = ArgumentCaptor.forClass(Configuration.class);
    mockedConnectionRegistryFactory
      .verify(() -> ConnectionRegistryFactory.create(any(), captor.capture(), any()));
    Configuration c = captor.getValue();
    assertEquals("1", c.get("a"));
    assertEquals("2", c.get("b"));
    assertEquals("", c.get("c"));
  }
}
