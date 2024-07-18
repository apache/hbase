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
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

/**
 * Make sure we can successfully parse the URI component
 */
@Category({ ClientTests.class, SmallTests.class })
public class TestConnectionRegistryUriParsing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConnectionRegistryUriParsing.class);

  private Configuration conf;

  private User user;

  private MockedConstruction<RpcConnectionRegistry> mockedRpcRegistry;

  private MockedConstruction<ZKConnectionRegistry> mockedZkRegistry;

  private MockedStatic<ReflectionUtils> mockedReflectionUtils;

  private List<?> args;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    user = mock(User.class);
    args = null;
    mockedRpcRegistry = mockConstruction(RpcConnectionRegistry.class, (mock, context) -> {
      args = context.arguments();
    });
    mockedZkRegistry = mockConstruction(ZKConnectionRegistry.class, (mock, context) -> {
      args = context.arguments();
    });
    mockedReflectionUtils = mockStatic(ReflectionUtils.class);
  }

  @After
  public void tearDown() {
    mockedRpcRegistry.closeOnDemand();
    mockedZkRegistry.closeOnDemand();
    mockedReflectionUtils.closeOnDemand();
  }

  @Test
  public void testParseRpcSingle() throws Exception {
    ConnectionRegistryFactory.create(new URI("hbase+rpc://server1:123"), conf, user);
    assertEquals(1, mockedRpcRegistry.constructed().size());
    assertSame(user, args.get(1));
    Configuration conf = (Configuration) args.get(0);
    assertEquals("server1:123", conf.get(RpcConnectionRegistry.BOOTSTRAP_NODES));
  }

  @Test
  public void testParseRpcMultiple() throws Exception {
    ConnectionRegistryFactory.create(new URI("hbase+rpc://server1:123,server2:456,server3:789"),
      conf, user);
    assertEquals(1, mockedRpcRegistry.constructed().size());
    assertSame(user, args.get(1));
    Configuration conf = (Configuration) args.get(0);
    assertEquals("server1:123,server2:456,server3:789",
      conf.get(RpcConnectionRegistry.BOOTSTRAP_NODES));
  }

  @Test
  public void testParseZkSingle() throws Exception {
    ConnectionRegistryFactory.create(new URI("hbase+zk://server1:123/root"), conf, user);
    assertEquals(1, mockedZkRegistry.constructed().size());
    assertSame(user, args.get(1));
    Configuration conf = (Configuration) args.get(0);
    assertEquals("server1:123", conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM));
    assertEquals("/root", conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }

  @Test
  public void testParseZkMultiple() throws Exception {
    ConnectionRegistryFactory
      .create(new URI("hbase+zk://server1:123,server2:456,server3:789/root/path"), conf, user);
    assertEquals(1, mockedZkRegistry.constructed().size());
    assertSame(user, args.get(1));
    Configuration conf = (Configuration) args.get(0);
    assertEquals("server1:123,server2:456,server3:789",
      conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM));
    assertEquals("/root/path", conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }

  @Test
  public void testFallbackNoScheme() throws Exception {
    conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, ZKConnectionRegistry.class,
      ConnectionRegistry.class);
    ConnectionRegistryFactory.create(new URI("server1:2181/path"), conf, user);
    ArgumentCaptor<Class<?>> clazzCaptor = ArgumentCaptor.forClass(Class.class);
    ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
    mockedReflectionUtils
      .verify(() -> ReflectionUtils.newInstance(clazzCaptor.capture(), argsCaptor.capture()));
    assertEquals(ZKConnectionRegistry.class, clazzCaptor.getValue());
    assertSame(conf, argsCaptor.getValue()[0]);
    assertSame(user, argsCaptor.getValue()[1]);
  }

  @Test
  public void testFallbackNoCreator() throws Exception {
    conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, RpcConnectionRegistry.class,
      ConnectionRegistry.class);
    ConnectionRegistryFactory.create(new URI("hbase+tls://server1:123/path"), conf, user);
    ArgumentCaptor<Class<?>> clazzCaptor = ArgumentCaptor.forClass(Class.class);
    ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
    mockedReflectionUtils
      .verify(() -> ReflectionUtils.newInstance(clazzCaptor.capture(), argsCaptor.capture()));
    assertEquals(RpcConnectionRegistry.class, clazzCaptor.getValue());
    assertSame(conf, argsCaptor.getValue()[0]);
    assertSame(user, argsCaptor.getValue()[1]);
  }
}
