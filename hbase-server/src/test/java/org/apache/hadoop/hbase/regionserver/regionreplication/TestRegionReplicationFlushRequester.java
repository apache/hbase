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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicationFlushRequester {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicationFlushRequester.class);

  private Configuration conf;

  private Runnable requester;

  private RegionReplicationFlushRequester flushRequester;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    conf.setInt(RegionReplicationFlushRequester.MIN_INTERVAL_SECS, 1);
    requester = mock(Runnable.class);
    flushRequester = new RegionReplicationFlushRequester(conf, requester);
  }

  @Test
  public void testRequest() throws InterruptedException {
    // should call request directly
    flushRequester.requestFlush(100L);
    verify(requester, times(1)).run();

    // should not call request directly, since the min interval is 1 second
    flushRequester.requestFlush(200L);
    verify(requester, times(1)).run();
    Thread.sleep(2000);
    verify(requester, times(2)).run();

    // should call request directly because we have already elapsed more than 1 second
    Thread.sleep(2000);
    flushRequester.requestFlush(300L);
    verify(requester, times(3)).run();
  }

  @Test
  public void testCancelFlushRequest() throws InterruptedException {
    flushRequester.requestFlush(100L);
    flushRequester.requestFlush(200L);
    verify(requester, times(1)).run();

    // the pending flush request should be canceled
    flushRequester.recordFlush(300L);
    Thread.sleep(2000);
    verify(requester, times(1)).run();
  }
}
