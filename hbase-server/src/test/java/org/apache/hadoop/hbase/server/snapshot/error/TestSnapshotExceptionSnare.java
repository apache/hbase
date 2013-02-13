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
package org.apache.hadoop.hbase.server.snapshot.error;

import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the exception snare propagates errors as expected
 */
@Category(SmallTests.class)
public class TestSnapshotExceptionSnare {

  private static final Log LOG = LogFactory.getLog(TestSnapshotExceptionSnare.class);

  /**
   * This test ensures that we only propagate snapshot exceptions, even if we don't get a snapshot
   * exception
   */
  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testPropagatesOnlySnapshotException() {
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("snapshot").build();
    ExceptionListener snare = new SnapshotExceptionSnare(snapshot);
    snare.receiveError("Some message", new Exception());
    try {
      ((SnapshotExceptionSnare) snare).failOnError();
      fail("Snare didn't throw an exception");
    } catch (HBaseSnapshotException e) {
      LOG.error("Correctly got a snapshot exception" + e);
    }
  }

  @Test
  public void testPropatesTimerError() {
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("snapshot").build();
    SnapshotExceptionSnare snare = new SnapshotExceptionSnare(snapshot);
    Configuration conf = new Configuration();
    // don't let the timer count down before we fire it off
    conf.setLong(SnapshotDescriptionUtils.MASTER_WAIT_TIME_DISABLED_SNAPSHOT, Long.MAX_VALUE);
    OperationAttemptTimer timer = TakeSnapshotUtils.getMasterTimerAndBindToMonitor(snapshot, conf,
      snare);
    timer.trigger();
    try {
      snare.failOnError();
    } catch (HBaseSnapshotException e) {
      LOG.info("Correctly failed from timer:" + e.getMessage());
    }
  }
}
