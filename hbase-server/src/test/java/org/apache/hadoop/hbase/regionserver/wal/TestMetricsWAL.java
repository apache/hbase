/**
 *
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

package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Category(SmallTests.class)
public class TestMetricsWAL {
  @Test
  public void testLogRollRequested() throws Exception {
    MetricsWALSource source = mock(MetricsWALSourceImpl.class);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    metricsWAL.logRollRequested(false);
    metricsWAL.logRollRequested(true);

    // Log roll was requested twice
    verify(source, times(2)).incrementLogRollRequested();
    // One was because of low replication on the hlog.
    verify(source, times(1)).incrementLowReplicationLogRoll();
  }

  @Test
  public void testPostSync() throws Exception {
    long nanos = TimeUnit.MILLISECONDS.toNanos(145);
    MetricsWALSource source = mock(MetricsWALSourceImpl.class);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    metricsWAL.postSync(nanos, 1);
    verify(source, times(1)).incrementSyncTime(145);
  }

  @Test
  public void testWalWrittenInBytes() throws Exception {
    MetricsWALSource source = mock(MetricsWALSourceImpl.class);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    metricsWAL.postAppend(100, 900);
    metricsWAL.postAppend(200, 2000);
    verify(source, times(1)).incrementWrittenBytes(100);
    verify(source, times(1)).incrementWrittenBytes(200);
  }

}
