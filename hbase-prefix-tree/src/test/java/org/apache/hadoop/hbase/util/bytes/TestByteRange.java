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

package org.apache.hadoop.hbase.util.bytes;

import junit.framework.Assert;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.SimpleByteRange;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestByteRange {

  @Test
  public void testConstructor() {
    ByteRange b = new SimpleByteRange(new byte[] { 0, 1, 2 });
    Assert.assertEquals(3, b.getLength());
  }

}
