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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSimpleByteRange {

  @Test
  public void testEmpty(){
    Assert.assertTrue(SimpleByteRange.isEmpty(null));
    ByteRange r = new SimpleByteRange();
    Assert.assertTrue(SimpleByteRange.isEmpty(r));
    Assert.assertTrue(r.isEmpty());
    r.set(new byte[0]);
    Assert.assertEquals(0, r.getBytes().length);
    Assert.assertEquals(0, r.getOffset());
    Assert.assertEquals(0, r.getLength());
    Assert.assertTrue(Bytes.equals(new byte[0], r.deepCopyToNewArray()));
    Assert.assertEquals(0, r.compareTo(new SimpleByteRange(new byte[0], 0, 0)));
    Assert.assertEquals(0, r.hashCode());
  }

  @Test
  public void testBasics() {
    ByteRange r = new SimpleByteRange(new byte[] { 1, 3, 2 });
    Assert.assertFalse(SimpleByteRange.isEmpty(r));
    Assert.assertNotNull(r.getBytes());//should be empty byte[], but could change this behavior
    Assert.assertEquals(3, r.getBytes().length);
    Assert.assertEquals(0, r.getOffset());
    Assert.assertEquals(3, r.getLength());

    //cloning (deep copying)
    Assert.assertTrue(Bytes.equals(new byte[]{1, 3, 2}, r.deepCopyToNewArray()));
    Assert.assertNotSame(r.getBytes(), r.deepCopyToNewArray());

    //hash code
    Assert.assertTrue(r.hashCode() > 0);
    Assert.assertEquals(r.hashCode(), r.deepCopy().hashCode());

    //copying to arrays
    byte[] destination = new byte[]{-59};//junk
    r.deepCopySubRangeTo(2, 1, destination, 0);
    Assert.assertTrue(Bytes.equals(new byte[]{2}, destination));

    //set length
    r.setLength(1);
    Assert.assertTrue(Bytes.equals(new byte[]{1}, r.deepCopyToNewArray()));
    r.setLength(2);//verify we retained the 2nd byte, but dangerous in real code
    Assert.assertTrue(Bytes.equals(new byte[]{1, 3}, r.deepCopyToNewArray()));
  }
}
