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
package org.apache.hadoop.hbase.procedure2;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestLockAndQueue {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLockAndQueue.class);

  @Test
  public void testHasLockAccess() {
    Map<Long, NoopProcedure<Void>> procMap = new HashMap<>();
    for (long i = 1; i <= 10; i++) {
      NoopProcedure<Void> proc = new NoopProcedure<>();
      proc.setProcId(i);
      if (i > 1) {
        proc.setParentProcId(i - 1);
        proc.setRootProcId(1);
      }
      procMap.put(i, proc);
    }
    LockAndQueue laq = new LockAndQueue(procMap::get);
    for (long i = 1; i <= 10; i++) {
      assertFalse(laq.hasLockAccess(procMap.get(i)));
    }
    for (long i = 1; i <= 10; i++) {
      NoopProcedure<Void> procHasLock = procMap.get(i);
      laq.tryExclusiveLock(procHasLock);
      for (long j = 1; j < i; j++) {
        assertFalse(laq.hasLockAccess(procMap.get(j)));
      }
      for (long j = i; j <= 10; j++) {
        assertTrue(laq.hasLockAccess(procMap.get(j)));
      }
      laq.releaseExclusiveLock(procHasLock);
    }
  }
}
