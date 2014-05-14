/**
 * Copyright 2014 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import junit.framework.Assert;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This tests the serialization and deserialization of HRegionLocation.
 */
@Category(SmallTests.class)
public class TestHRegionLocation {
  @Test
  public void testSwiftSerDe() throws Exception {
    HRegionLocation location = createDummyHRegionLocation();
    byte[] serializedLocation = Bytes.writeThriftBytes(location,
      HRegionLocation.class);

    HRegionLocation locationCopy = Bytes.readThriftBytes(serializedLocation,
      HRegionLocation.class);

    Assert.assertEquals("HRegionLocation did not serialize correctly",
      location, locationCopy);
  }


  private static HRegionLocation createDummyHRegionLocation() {
    HRegionInfo hri = new HRegionInfo(
      new HTableDescriptor(Bytes.toBytes("table")),
      Bytes.toBytes("aaa"),
      Bytes.toBytes("zzz"),
      false,
      123456);
    return new HRegionLocation(hri, new HServerAddress("localhost", 1234));
  }
}
