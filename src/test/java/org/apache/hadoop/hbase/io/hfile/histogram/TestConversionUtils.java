/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.io.hfile.histogram;

import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import static org.apache.hadoop.hbase.io.hfile.histogram.UniformSplitHFileHistogram.*;
import static org.junit.Assert.*;

public class TestConversionUtils {

  @Test
  public void testDoubleConversion() {
    double d = convertBytesToDouble(getPaddedInfinityArr());
    assertTrue("d:" + d, d > 0.0);
    byte[] b = convertDoubleToBytes(d);
    assertTrue(b.length == PADDING);

    for (int i = 0; i < 1; i++) {
      testDoubleConversionOnce();
    }
  }

  public void testDoubleConversionOnce() {
    Random r = new Random();
    byte[] arr = new byte[PADDING];
    r.nextBytes(arr);

    double d = convertBytesToDouble(arr);
    byte[] arrret = convertDoubleToBytes(d);
    assertTrue("arr: " + Bytes.toStringBinary(arr) + ", arrret : "
        + Bytes.toStringBinary(arrret), Bytes.longestCommonPrefix(arr, arrret)
        >= 4);
  }

}
