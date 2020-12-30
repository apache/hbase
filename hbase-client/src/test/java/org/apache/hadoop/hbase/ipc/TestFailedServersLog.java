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
package org.apache.hadoop.hbase.ipc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Category({ ClientTests.class, SmallTests.class })
public class TestFailedServersLog {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFailedServersLog.class);

  static final int TEST_PORT = 9999;
  private InetSocketAddress addr;

  @Mock
  private Appender mockAppender;

  @Captor
  private ArgumentCaptor captorLoggingEvent;

  @Before
  public void setup() {
    LogManager.getRootLogger().addAppender(mockAppender);
  }

  @After
  public void teardown() {
    LogManager.getRootLogger().removeAppender(mockAppender);
  }

  @Test
  public void testAddToFailedServersLogging() {
    Throwable nullException = new NullPointerException();

    FailedServers fs = new FailedServers(new Configuration());
    addr = new InetSocketAddress(TEST_PORT);

    fs.addToFailedServers(addr, nullException);

    Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
    LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
    assertThat(loggingEvent.getLevel(), is(Level.DEBUG));
    assertEquals("Added failed server with address " + addr.toString() + " to list caused by "
        + nullException.toString(),
      loggingEvent.getRenderedMessage());
  }

}
