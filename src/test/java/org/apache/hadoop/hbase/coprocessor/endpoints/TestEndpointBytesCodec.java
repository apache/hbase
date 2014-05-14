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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.util.ArrayList;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcases for EndpointBytesCodec.
 */
@Category(SmallTests.class)
public class TestEndpointBytesCodec {
  @Test
  public void testBasic() throws Exception {
    final Object[] VALUES = {
        false, true,
        (byte) 2,
        (char) 3,
        (short) 4,
        (int) 5,
        (long) 6,
        (float) 7.,
        (double) 8.,
        Bytes.toBytes("9"),
    };

    ArrayList<byte[]> arrayBytes = EndpointBytesCodec.encodeArray(VALUES);
    for (int i = 0; i < VALUES.length; i++) {
      Object vl = VALUES[i];
      byte[] bytes = EndpointBytesCodec.encodeObject(vl);

      Assert.assertArrayEquals("element in encodedArray[" + i + "]", bytes,
          arrayBytes.get(i));

      Object decoded = EndpointBytesCodec.decode(vl.getClass(), bytes);
      if (vl instanceof byte[]) {
        Assert.assertArrayEquals(
            "recoved value of " + Bytes.toString((byte[]) vl),
            (byte[]) vl, (byte[]) decoded);
      } else {
        Assert.assertEquals("recoved value of " + vl, vl, decoded);
      }
    }

  }
}
