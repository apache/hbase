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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.MetricsConnection.CallStats;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.NotSslRecordException;

/**
 * A simple UT to make sure that we do not leak the SslExceptions to netty's TailContext, where it
 * will generate a confusing WARN message.
 * <p>
 * See HBASE-27782 for more details.
 */
@Category({ ClientTests.class, SmallTests.class })
public class TestTLSHandshadeFailure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTLSHandshadeFailure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTLSHandshadeFailure.class);

  private final Configuration conf = HBaseConfiguration.create();

  // use a pre set seed to make the random bytes stable
  private final Random rand = new Random(1);

  private ServerSocket server;

  private Thread serverThread;

  private NettyRpcClient client;

  private org.apache.logging.log4j.core.Appender mockAppender;

  private void serve() {
    Socket socket = null;
    try {
      socket = server.accept();
      byte[] bytes = new byte[128];
      rand.nextBytes(bytes);
      socket.getOutputStream().write(bytes);
      socket.getOutputStream().flush();
    } catch (Exception e) {
      LOG.warn("failed to process request", e);
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e1) {
          LOG.warn("failed to close socket");
        }
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    server = new ServerSocket(0);
    serverThread = new Thread(this::serve);
    serverThread.setDaemon(true);
    serverThread.setName("Error-Server-Thread");
    serverThread.start();
    conf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, true);
    client = new NettyRpcClient(conf);

    mockAppender = mock(org.apache.logging.log4j.core.Appender.class);
    when(mockAppender.getName()).thenReturn("mockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(BufferCallBeforeInitHandler.class)).addAppender(mockAppender);
  }

  @After
  public void tearDown() throws IOException {
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger(BufferCallBeforeInitHandler.class)).removeAppender(mockAppender);
    Closeables.close(client, true);
    Closeables.close(server, true);
  }

  @Test
  public void test() throws Exception {
    AtomicReference<org.apache.logging.log4j.Level> level = new AtomicReference<>();
    AtomicReference<String> msg = new AtomicReference<String>();
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        org.apache.logging.log4j.core.LogEvent logEvent =
          invocation.getArgument(0, org.apache.logging.log4j.core.LogEvent.class);
        level.set(logEvent.getLevel());
        msg.set(logEvent.getMessage().getFormattedMessage());
        return null;
      }
    }).when(mockAppender).append(any());
    ConnectionId id = new ConnectionId(User.getCurrent(), "test",
      Address.fromParts("127.0.0.1", server.getLocalPort()));
    NettyRpcConnection conn = client.createConnection(id);
    BlockingRpcCallback<Call> done = new BlockingRpcCallback<>();
    Call call =
      new Call(1, null, null, null, null, 0, 0, Collections.emptyMap(), done, new CallStats());
    HBaseRpcController hrc = new HBaseRpcControllerImpl();
    conn.sendRequest(call, hrc);
    done.get();
    assertThat(call.error, instanceOf(NotSslRecordException.class));
    Waiter.waitFor(conf, 5000, () -> msg.get() != null);
    verify(mockAppender).append(any());
    // make sure that it has been logged by BufferCallBeforeInitHandler
    assertEquals(org.apache.logging.log4j.Level.DEBUG, level.get());
    assertThat(msg.get(),
      startsWith("got ssl exception, which should have already been proceeded"));
  }
}
