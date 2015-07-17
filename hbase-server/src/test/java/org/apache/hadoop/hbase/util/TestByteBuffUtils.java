/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestByteBuffUtils {
  @Test
  public void testCopyAndCompare() throws Exception {
    ByteBuffer bb1 = ByteBuffer.allocate(50);
    ByteBuffer bb2 = ByteBuffer.allocate(50);
    MultiByteBuff src = new MultiByteBuff(bb1, bb2);
    for (int i = 0; i < 7; i++) {
      src.putLong(8l);
    }
    src.put((byte) 1);
    src.put((byte) 1);
    ByteBuffer bb3 = ByteBuffer.allocate(50);
    ByteBuffer bb4 = ByteBuffer.allocate(50);
    MultiByteBuff mbbDst = new MultiByteBuff(bb3, bb4);
    // copy from MBB to MBB
    mbbDst.put(0, src, 0, 100);
    int compareTo = ByteBuff.compareTo(src, 0, 100, mbbDst, 0, 100);
    assertTrue(compareTo == 0);
    // Copy from MBB to SBB
    bb3 = ByteBuffer.allocate(100);
    SingleByteBuff sbbDst = new SingleByteBuff(bb3);
    src.rewind();
    sbbDst.put(0, src, 0, 100);
    compareTo = ByteBuff.compareTo(src, 0, 100, sbbDst, 0, 100);
    assertTrue(compareTo == 0);
    // Copy from SBB to SBB
    bb3 = ByteBuffer.allocate(100);
    SingleByteBuff sbb = new SingleByteBuff(bb3);
    for (int i = 0; i < 7; i++) {
      sbb.putLong(8l);
    }
    sbb.put((byte) 1);
    sbb.put((byte) 1);
    bb4 = ByteBuffer.allocate(100);
    sbbDst = new SingleByteBuff(bb4);
    sbbDst.put(0, sbb, 0, 100);
    compareTo = ByteBuff.compareTo(sbb, 0, 100, sbbDst, 0, 100);
    assertTrue(compareTo == 0);
    // copy from SBB to MBB
    sbb.rewind();
    mbbDst = new MultiByteBuff(bb3, bb4);
    mbbDst.rewind();
    mbbDst.put(0, sbb, 0, 100);
    compareTo = ByteBuff.compareTo(sbb, 0, 100, mbbDst, 0, 100);
    assertTrue(compareTo == 0);
  }
}
