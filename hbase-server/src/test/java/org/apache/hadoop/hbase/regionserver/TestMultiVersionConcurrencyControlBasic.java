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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Very basic tests.
 * @see TestMultiVersionConcurrencyControl for more.
 */
@Category({SmallTests.class})
public class TestMultiVersionConcurrencyControlBasic {
  @Test
  public void testSimpleMvccOps() {
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    long readPoint = mvcc.memstoreReadPoint();
    MultiVersionConcurrencyControl.WriteEntry writeEntry = mvcc.beginMemstoreInsert();
    mvcc.completeMemstoreInsert(writeEntry);
    long readPoint2 = mvcc.memstoreReadPoint();
    assertEquals(readPoint, readPoint2);
    long seqid = 238;
    writeEntry = mvcc.beginMemstoreInsertWithSeqNum(seqid);
    mvcc.completeMemstoreInsert(writeEntry);
    assertEquals(seqid, mvcc.memstoreReadPoint());
    writeEntry = mvcc.beginMemstoreInsertWithSeqNum(seqid + 1);
    assertTrue(mvcc.advanceMemstore(writeEntry));
    assertEquals(seqid + 1, mvcc.memstoreReadPoint());
  }

  @Test
  public void testCancel() {
    long seqid = 238;
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    MultiVersionConcurrencyControl.WriteEntry writeEntry =
      mvcc.beginMemstoreInsertWithSeqNum(seqid);
    assertTrue(mvcc.advanceMemstore(writeEntry));
    assertEquals(seqid, mvcc.memstoreReadPoint());
    writeEntry = mvcc.beginMemstoreInsertWithSeqNum(seqid + 1);
    mvcc.cancelMemstoreInsert(writeEntry);
    assertEquals(seqid, mvcc.memstoreReadPoint());
  }
}
