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
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDataBlockEncoding {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDataBlockEncoding.class);

  @Test
  public void testGetDataBlockEncoder() throws Exception {
    for (DataBlockEncoding algo : DataBlockEncoding.values()) {
      DataBlockEncoder encoder = DataBlockEncoding.getDataBlockEncoderById(algo.getId());
      if (algo.getId() != 0) {
        assertTrue(DataBlockEncoding.isCorrectEncoder(encoder, algo.getId()));
      }
    }
    try {
      DataBlockEncoding.getDataBlockEncoderById((short) -1);
      fail("Illegal encoderId, should get IllegalArgumentException.");
    } catch (IllegalArgumentException ie) {
    }
    try {
      DataBlockEncoding.getDataBlockEncoderById(Byte.MAX_VALUE);
      // fail because idArray[Byte.MAX_VALUE] = null
      fail("Illegal encoderId, should get IllegalArgumentException.");
    } catch (IllegalArgumentException ie) {
    }
  }

}
