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
package org.apache.hadoop.hbase.io.hfile.bucket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * Test for {@link ByteBufferIOEngine}
 */
@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestByteBufferIOEngine {

  private static final Log LOG = LogFactory.getLog(TestByteBufferIOEngine.class);

  private final boolean isDirect;

  public TestByteBufferIOEngine(boolean isDirect) {
    this.isDirect = isDirect;
    LOG.info("Running with direct allocation " +
        (isDirect? "enabled" : "disabled"));
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getConfigurations() {
    Object[][] data = new Object[][] { {true}, {false} };
    return Arrays.asList(data);
  }

  @Test
  public void testByteBufferIOEngine() throws Exception {
    int capacity = 32 * 1024 * 1024; // 32 MB
    int testNum = 100;
    int maxBlockSize = 64 * 1024;
    ByteBufferIOEngine ioEngine = new ByteBufferIOEngine(capacity,
        2 * 1024 * 1024, isDirect);
    int testOffsetAtStartNum = testNum / 10;
    int testOffsetAtEndNum = testNum / 10;
    for (int i = 0; i < testNum; i++) {
      byte val = (byte) (Math.random() * 255);
      int blockSize = (int) (Math.random() * maxBlockSize);
      byte[] byteArray = new byte[blockSize];
      for (int j = 0; j < byteArray.length; ++j) {
        byteArray[j] = val;
      }
      int offset = 0;
      if (testOffsetAtStartNum > 0) {
        testOffsetAtStartNum--;
        offset = 0;
      } else if (testOffsetAtEndNum > 0) {
        testOffsetAtEndNum--;
        offset = capacity - blockSize;
      } else {
        offset = (int) (Math.random() * (capacity - maxBlockSize));
      }
      ioEngine.write(byteArray, offset);
      byte[] dst = new byte[blockSize];
      ioEngine.read(dst, offset);
      for (int j = 0; j < byteArray.length; ++j) {
        assertTrue(byteArray[j] == dst[j]);
      }
    }
    assert testOffsetAtStartNum == 0;
    assert testOffsetAtEndNum == 0;
  }
}
