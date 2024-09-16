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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGeneratorCopy;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslHandler;

@Category({ RPCTests.class, MediumTests.class })
@RunWith(Parameterized.class)
public class TestNettyRpcServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNettyRpcServer.class);

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int NUM_ROWS = 100;
  private static final int MIN_LEN = 1000;
  private static final int MAX_LEN = 1000000;
  protected static final LoadTestKVGeneratorCopy GENERATOR =
    new LoadTestKVGeneratorCopy(MIN_LEN, MAX_LEN);
  protected static HBaseTestingUtil TEST_UTIL;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @Parameterized.Parameter
  public String allocatorType;

  @Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] { { NettyRpcServer.POOLED_ALLOCATOR_TYPE },
      { NettyRpcServer.UNPOOLED_ALLOCATOR_TYPE }, { NettyRpcServer.HEAP_ALLOCATOR_TYPE },
      { SimpleByteBufAllocator.class.getName() } });
  }

  @Before
  public void setup() throws Exception {
    // A subclass may have already created TEST_UTIL and is now upcalling to us
    if (TEST_UTIL == null) {
      TEST_UTIL = new HBaseTestingUtil();
    }
    TEST_UTIL.getConfiguration().set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
      NettyRpcServer.class.getName());
    TEST_UTIL.getConfiguration().set(NettyRpcServer.HBASE_NETTY_ALLOCATOR_KEY, allocatorType);
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNettyRpcServer() throws Exception {
    doTest(name.getTableName());
  }

  protected void doTest(TableName tableName) throws Exception {
    // Splitting just complicates the test scenario, disable it
    final TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setRegionSplitPolicyClassName(DisabledRegionSplitPolicy.class.getName()).build();
    try (Table table =
      TEST_UTIL.createTable(desc, new byte[][] { FAMILY }, TEST_UTIL.getConfiguration())) {
      // put some test data
      for (int i = 0; i < NUM_ROWS; i++) {
        final byte[] rowKey = Bytes.toBytes(LoadTestKVGeneratorCopy.md5PrefixedKey(i));
        final byte[] v = GENERATOR.generateRandomSizeValue(rowKey, QUALIFIER);
        table.put(new Put(rowKey).addColumn(FAMILY, QUALIFIER, v));
      }
      // read to verify it.
      for (int i = 0; i < NUM_ROWS; i++) {
        final byte[] rowKey = Bytes.toBytes(LoadTestKVGeneratorCopy.md5PrefixedKey(i));
        final Result r = table.get(new Get(rowKey).addColumn(FAMILY, QUALIFIER));
        assertNotNull("Result was empty", r);
        final byte[] v = r.getValue(FAMILY, QUALIFIER);
        assertNotNull("Result did not contain expected value", v);
        assertTrue("Value was not verified", LoadTestKVGeneratorCopy.verify(v, rowKey, QUALIFIER));
      }
    }
  }

  private static final String CERTIFICATE = "-----BEGIN CERTIFICATE-----\n"
    + "MIIEITCCAwmgAwIBAgIUaLL8vLOhWLCLXVHEJqXJhfmsTB8wDQYJKoZIhvcNAQEL\n"
    + "BQAwgawxCzAJBgNVBAYTAlVTMRYwFAYDVQQIDA1NYXNzYWNodXNldHRzMRIwEAYD\n"
    + "VQQHDAlDYW1icmlkZ2UxGDAWBgNVBAoMD25ldHR5IHRlc3QgY2FzZTEYMBYGA1UE\n"
    + "CwwPbmV0dHkgdGVzdCBjYXNlMRgwFgYDVQQDDA9uZXR0eSB0ZXN0IGNhc2UxIzAh\n"
    + "BgkqhkiG9w0BCQEWFGNjb25uZWxsQGh1YnNwb3QuY29tMB4XDTI0MDEyMTE5MzMy\n"
    + "MFoXDTI1MDEyMDE5MzMyMFowgawxCzAJBgNVBAYTAlVTMRYwFAYDVQQIDA1NYXNz\n"
    + "YWNodXNldHRzMRIwEAYDVQQHDAlDYW1icmlkZ2UxGDAWBgNVBAoMD25ldHR5IHRl\n"
    + "c3QgY2FzZTEYMBYGA1UECwwPbmV0dHkgdGVzdCBjYXNlMRgwFgYDVQQDDA9uZXR0\n"
    + "eSB0ZXN0IGNhc2UxIzAhBgkqhkiG9w0BCQEWFGNjb25uZWxsQGh1YnNwb3QuY29t\n"
    + "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy+qzEZpQMjVdLj0siUcG\n"
    + "y8LIHOW4S+tgHIKFkF865qWq6FVGbROe2Z0f5W6yIamZkdxzptT0iv+8S5okNNeW\n"
    + "2NbsN/HNJIRtWfxku1Jh1gBqSkAYIjXyq7+20hIaJTzzxqike9M/Lc14EGb33Ja/\n"
    + "kDPRV3UtiM3Ntf3eALXKbrWptkbgQngCaTgtfg8IkMAEpP270wZ9fW0lDHv3NPPt\n"
    + "Zt0QSJzWSqWfu+l4ayvcUQYyNJesx9YmTHSJu69lvT4QApoX8FEiHfNCJ28R50CS\n"
    + "aIgOpCWUvkH7rqx0p9q393uJRS/S6RlLbU30xUN1fNrVmP/XAapfy+R0PSgiUi8o\n"
    + "EQIDAQABozkwNzAWBgNVHRIEDzANggt3d3cuZm9vLmNvbTAdBgNVHQ4EFgQUl4FD\n"
    + "Y8jJ/JHJR68YqPsGUjUJuwgwDQYJKoZIhvcNAQELBQADggEBADVzivYz2M0qsWUc\n"
    + "jXjCHymwTIr+7ud10um53FbYEAfKWsIY8Pp35fKpFzUwc5wVdCnLU86K/YMKRzNB\n"
    + "zL2Auow3PJFRvXecOv7dWxNlNneLDcwbVrdNRu6nQXmZUgyz0oUKuJbF+JGtI+7W\n"
    + "kRw7yhBfki+UCSQWeDqvaWzgmA4Us0N8NFq3euAs4xFbMMPMQWrT9Z7DGchCeRiB\n"
    + "dkQBvh88vbR3v2Saq14W4Wt5rj2++vXWGQSeAQL6nGbOwc3ohW6isNNV0eGQQTmS\n"
    + "khS2d/JDZq2XL5RGexf3CA6YYzWiTr9YZHNjuobvLH7mVnA2c8n6Zty/UhfnuK1x\n" + "JbkleFk=\n"
    + "-----END CERTIFICATE-----";

  @Test
  public void testHandshakeCompleteHandler()
    throws SSLPeerUnverifiedException, CertificateException {
    NettyServerRpcConnection conn = mock(NettyServerRpcConnection.class);
    SslHandler sslHandler = mock(SslHandler.class);
    SocketAddress remoteAddress = new InetSocketAddress("localhost", 5555);
    SSLEngine engine = mock(SSLEngine.class);
    SSLSession session = mock(SSLSession.class);
    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    X509Certificate x509Certificate = (X509Certificate) certificateFactory
      .generateCertificate(new ByteArrayInputStream(CERTIFICATE.getBytes(StandardCharsets.UTF_8)));
    Certificate[] certificates = new Certificate[] { x509Certificate };

    when(sslHandler.engine()).thenReturn(engine);
    when(engine.getSession()).thenReturn(session);
    when(session.getPeerCertificates()).thenReturn(certificates);

    NettyRpcServer.sslHandshakeCompleteHandler(conn, sslHandler, remoteAddress);

    assertArrayEquals(certificates, conn.clientCertificateChain);
  }

}
