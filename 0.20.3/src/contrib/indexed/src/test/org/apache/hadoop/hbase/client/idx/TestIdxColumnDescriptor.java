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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;
import java.util.Set;

/**
 * Tests the {@link org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor}.
 */
public class TestIdxColumnDescriptor extends TestCase {
  /**
   * Tests the {@link org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor#addIndexDescriptor(IdxIndexDescriptor)}
   * method.
   */
  public void testAddIndexDescriptor() throws IOException {
    IdxColumnDescriptor descriptor = new IdxColumnDescriptor("familyName");
    IdxIndexDescriptor indexDescriptor = new IdxIndexDescriptor(Bytes.toBytes("qualifer"), IdxQualifierType.INT);

    Assert.assertEquals("The column desciptor should not contain any index descriptors",
        0, descriptor.getIndexDescriptors().size());
    descriptor.addIndexDescriptor(indexDescriptor);
    Assert.assertEquals("The column desciptor should contain a index descriptor",
        1, descriptor.getIndexDescriptors().size());
  }

  /**
   * Tests the {@link org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor#addIndexDescriptor(IdxIndexDescriptor)}
   * method when an index descriptor already exists for the qualifier.
   */
  public void testAddIndexDescriptorWithExisting() throws IOException {
    IdxColumnDescriptor descriptor = new IdxColumnDescriptor("familyName");
    IdxIndexDescriptor indexDescriptor = new IdxIndexDescriptor(Bytes.toBytes("qualifer"), IdxQualifierType.INT);
    IdxIndexDescriptor indexDescriptor2 = new IdxIndexDescriptor(Bytes.toBytes("qualifer"), IdxQualifierType.LONG);

    descriptor.addIndexDescriptor(indexDescriptor);
    try {
      descriptor.addIndexDescriptor(indexDescriptor2);
      Assert.fail("An exception should have been thrown");
    } catch (Exception e) {
      Assert.assertEquals("Wrong exception thrown", IllegalArgumentException.class, e.getClass());
    }
  }

  /**
   * Tests the {@link org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor#removeIndexDescriptor(byte[])}
   * method.
   */
  public void testRemoveIndexDescriptor() throws IOException {
    IdxColumnDescriptor descriptor = new IdxColumnDescriptor("familyName");
    IdxIndexDescriptor indexDescriptor = new IdxIndexDescriptor(Bytes.toBytes("qualifer"), IdxQualifierType.INT);

    Assert.assertEquals("The column desciptor should not contain any index descriptors",
        0, descriptor.getIndexDescriptors().size());
    descriptor.addIndexDescriptor(indexDescriptor);
    Assert.assertEquals("The column desciptor should contain a index descriptor",
        1, descriptor.getIndexDescriptors().size());
    Assert.assertTrue("The remove method should have returned true",
        descriptor.removeIndexDescriptor(Bytes.toBytes("qualifer")));
    Assert.assertEquals("The column desciptor should not contain any index descriptors",
        0, descriptor.getIndexDescriptors().size());
  }

  /**
   * Tests the {@link IdxColumnDescriptor#getIndexedQualifiers()} method when an
   * index descriptor already exists for the qualifier.
   */
  public void testGetIndexedQualifiers() throws IOException {
    IdxColumnDescriptor descriptor = new IdxColumnDescriptor("familyName");
    byte[] qualifierName1 = Bytes.toBytes("qualifer1");
    IdxIndexDescriptor indexDescriptor1
        = new IdxIndexDescriptor(qualifierName1, IdxQualifierType.INT);
    byte[] qualifierName2 = Bytes.toBytes("qualifer2");
    IdxIndexDescriptor indexDescriptor2
        = new IdxIndexDescriptor(qualifierName2, IdxQualifierType.LONG);

    descriptor.addIndexDescriptor(indexDescriptor1);
    descriptor.addIndexDescriptor(indexDescriptor2);

    Set<ImmutableBytesWritable> indexedQualifiers = descriptor.getIndexedQualifiers();
    Assert.assertNotNull("The set of indexed qualifiers should not be null",
        indexedQualifiers);
    Assert.assertEquals("The column desciptor should contain index qualifiers",
        2, indexedQualifiers.size());

    Assert.assertTrue("The set of indexed qualifiers should contain the key",
        indexedQualifiers.contains(new ImmutableBytesWritable(qualifierName1)));
    Assert.assertTrue("The set of indexed qualifiers should contain the key",
        indexedQualifiers.contains(new ImmutableBytesWritable(qualifierName2)));
  }

  /**
   * Tests the {@link IdxColumnDescriptor#write(java.io.DataOutput)} and {@link
   * org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor#readFields(java.io.DataInput)}
   * methods.
   * @throws java.io.IOException if an error occurs
   */
  public void testWritable() throws IOException {
    IdxColumnDescriptor descriptor = new IdxColumnDescriptor("familyName");
    byte[] qualifierName1 = Bytes.toBytes("qualifer1");
    IdxIndexDescriptor indexDescriptor1
        = new IdxIndexDescriptor(qualifierName1, IdxQualifierType.INT);
    byte[] qualifierName2 = Bytes.toBytes("qualifer2");
    IdxIndexDescriptor indexDescriptor2
        = new IdxIndexDescriptor(qualifierName2, IdxQualifierType.LONG);

    descriptor.addIndexDescriptor(indexDescriptor1);
    descriptor.addIndexDescriptor(indexDescriptor2);

    DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
    descriptor.write(dataOutputBuffer);

    DataInputBuffer dataInputBuffer = new DataInputBuffer();
    dataInputBuffer.reset(dataOutputBuffer.getData(), dataOutputBuffer.getLength());

    IdxColumnDescriptor clonedDescriptor = new IdxColumnDescriptor();
    clonedDescriptor.readFields(dataInputBuffer);

    Assert.assertEquals("The expression was not the same after being written and read", descriptor, clonedDescriptor);
  }

  /**
   * Tests the {@link org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor#compareTo(org.apache.hadoop.hbase.HColumnDescriptor)}
   * method when the two instances are the same.
   */
  public void testCompareToWhenSame() throws IOException {
    IdxColumnDescriptor descriptor1 = new IdxColumnDescriptor("familyName1");
    byte[] qualifierName1 = Bytes.toBytes("qualifer1");
    IdxIndexDescriptor indexDescriptor1
        = new IdxIndexDescriptor(qualifierName1, IdxQualifierType.INT);
    descriptor1.addIndexDescriptor(indexDescriptor1);

    IdxColumnDescriptor descriptor2 = new IdxColumnDescriptor("familyName1");
    byte[] qualifierName2 = Bytes.toBytes("qualifer1");
    IdxIndexDescriptor indexDescriptor2
        = new IdxIndexDescriptor(qualifierName2, IdxQualifierType.INT);
    descriptor2.addIndexDescriptor(indexDescriptor2);

    Assert.assertTrue("The compare to should have returned 0", descriptor1.compareTo(descriptor2) == 0);
  }

  /**
   * Tests the {@link org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor#compareTo(org.apache.hadoop.hbase.HColumnDescriptor)}
   * method when the two instances are different.
   */
  public void testCompareToWhenDifferent() throws IOException {
    IdxColumnDescriptor descriptor1 = new IdxColumnDescriptor("familyName1");
    byte[] qualifierName1 = Bytes.toBytes("qualifer1");
    IdxIndexDescriptor indexDescriptor1
        = new IdxIndexDescriptor(qualifierName1, IdxQualifierType.INT);
    descriptor1.addIndexDescriptor(indexDescriptor1);

    IdxColumnDescriptor descriptor2 = new IdxColumnDescriptor("familyName2");
    byte[] qualifierName2 = Bytes.toBytes("qualifer2");
    IdxIndexDescriptor indexDescriptor2
        = new IdxIndexDescriptor(qualifierName2, IdxQualifierType.INT);
    descriptor2.addIndexDescriptor(indexDescriptor2);

    Assert.assertTrue("The compare to should not have returned 0", descriptor1.compareTo(descriptor2) != 0);
  }

  /**
   * Tests that two column descriptors are equal when the Idx decorator isn't being used.
   */
  public void testCompareToWithoutDecorator() throws IOException {
    IdxColumnDescriptor descriptor1 = new IdxColumnDescriptor("familyName1");
    byte[] qualifierName1 = Bytes.toBytes("qualifer1");
    IdxIndexDescriptor indexDescriptor1
        = new IdxIndexDescriptor(qualifierName1, IdxQualifierType.INT);
    descriptor1.addIndexDescriptor(indexDescriptor1);

    IdxColumnDescriptor descriptor2 = new IdxColumnDescriptor("familyName2");
    byte[] qualifierName2 = Bytes.toBytes("qualifer2");
    IdxIndexDescriptor indexDescriptor2
        = new IdxIndexDescriptor(qualifierName2, IdxQualifierType.INT);
    descriptor2.addIndexDescriptor(indexDescriptor2);

    HColumnDescriptor descriptor2WithoutDec = new HColumnDescriptor(descriptor2);

    Assert.assertTrue("The compare to should not have returned 0", descriptor1.compareTo(descriptor2WithoutDec) != 0);
  }
}
