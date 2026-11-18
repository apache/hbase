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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.IdReadWriteLockWithObjectPool.ReferenceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestIdReadWriteLockWithObjectPoolPurge {

  private static final int NUM_IDS = 16;

  @Test
  public void testGetLockSkipsPurgeWhenDisabled() {
    // Test with Weak refs b/c gc always clears them
    IdReadWriteLockWithObjectPool<Long> idLock =
      new IdReadWriteLockWithObjectPool<>(ReferenceType.WEAK, false);
    for (long id = 0; id < NUM_IDS; id++) {
      idLock.getLock(id);
    }
    System.gc();
    Threads.sleep(200);

    // Stale entries survive getLock
    ReentrantReadWriteLock liveLock = idLock.getLock((long) NUM_IDS);
    assertEquals(NUM_IDS + 1, idLock.getPoolSize());

    // Stale entries are removed by purge
    assertEquals(1, idLock.purgeAndGetEntryPoolSize());
    assertSame(liveLock, idLock.getLock((long) NUM_IDS));
  }
}
