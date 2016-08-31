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
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestBufferedDataBlockEncoder {

  @Test
  public void testEnsureSpaceForKey() {
    BufferedDataBlockEncoder.SeekerState state = new BufferedDataBlockEncoder.SeekerState();
    for (int i = 1; i <= 65536; ++i) {
      state.keyLength = i;
      state.ensureSpaceForKey();
      state.keyBuffer[state.keyLength - 1] = (byte) ((i - 1) % 0xff);
      for (int j = 0; j < i - 1; ++j) {
        // Check that earlier bytes were preserved as the buffer grew.
        assertEquals((byte) (j % 0xff), state.keyBuffer[j]);
      }
    }
  }

}
