/*
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
package org.apache.hadoop.hbase.client.idx;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

/**
 * Tests the {@link IdxIndexDescriptor} class.
 */
public class TestIdxIndexDescriptor extends TestCase {
  /**
   * Tests the {@link IdxIndexDescriptor#write(java.io.DataOutput)} and {@link
   * org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor#readFields(java.io.DataInput)}
   * methods.
   * @throws java.io.IOException if an error occurs
   */
  public void testWritable() throws IOException {
    final int repeatCount = 3;
    IdxIndexDescriptor descriptor = createIdxIndexDescriptor();

    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    for (int i = 0; i < repeatCount; i++) {
      descriptor.write(dataOutputBuffer);
    }

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    for (int i = 0; i < repeatCount; i++) {
      IdxIndexDescriptor clonedDescriptor = new IdxIndexDescriptor();
      clonedDescriptor.readFields(dataInputBuffer);
      Assert.assertEquals("The descriptor was not the same after being written and " +
        "read attempt=" + i, descriptor, clonedDescriptor);
    }
  }

  /**
   * Tests the equals method.
   */
  public void testEquals() {
    IdxIndexDescriptor ix1 = createIdxIndexDescriptor();

    IdxIndexDescriptor ix2 = createIdxIndexDescriptor();
    Assert.assertEquals(ix1, ix2);

    ix2.getQualifierName()[0]=9;
    Assert.assertFalse(ix1.equals(ix2));

    ix2 = createIdxIndexDescriptor();
    ix2.setQualifierType(IdxQualifierType.LONG);
    Assert.assertFalse(ix1.equals(ix2));

    ix2 = createIdxIndexDescriptor();
    ix2.setOffset(1);
    Assert.assertFalse(ix1.equals(ix2));

    ix2 = createIdxIndexDescriptor();
    ix2.setLength(-1);
    Assert.assertFalse(ix1.equals(ix2));

  }

  private static IdxIndexDescriptor createIdxIndexDescriptor() {
    byte[] qualifierName1 = Bytes.toBytes("qualifer1");
    IdxIndexDescriptor descriptor
      = new IdxIndexDescriptor(qualifierName1, IdxQualifierType.INT);
    descriptor.setLength(4);
    descriptor.setOffset(2);
    return descriptor;
  }


}
