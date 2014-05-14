/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.StopStatus;
import org.apache.hadoop.hbase.master.RegionServerOperationQueue.ProcessingResultCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the queue used to manage RegionServerOperations.
 * Currently RegionServerOperationQueue is untestable because each
 * RegionServerOperation has a {@link HMaster} reference.  TOOD: Fix.
 */
@Category(SmallTests.class)
public class TestRegionServerOperationQueue {
  private RegionServerOperationQueue queue;
  private Configuration conf;
  private StopStatus stopStatus = new StopStatus() {
    @Override
    public boolean isStopped() {
      return false;
    }
  };
  
  @Before
  public void setUp() throws Exception {
    this.conf = new Configuration();
    this.queue = new RegionServerOperationQueue(this.conf, null, stopStatus);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testWeDoNotGetStuckInDelayQueue() throws Exception {
    ProcessingResultCode code = this.queue.process();
    assertTrue(ProcessingResultCode.NOOP == code);
  }
}
