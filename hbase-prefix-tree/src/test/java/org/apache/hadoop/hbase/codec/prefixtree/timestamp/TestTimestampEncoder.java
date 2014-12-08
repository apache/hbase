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

package org.apache.hadoop.hbase.codec.prefixtree.timestamp;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.decode.timestamp.TimestampDecoder;
import org.apache.hadoop.hbase.codec.prefixtree.encode.other.LongEncoder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestTimestampEncoder {

  @Parameters
  public static Collection<Object[]> parameters() {
    return new TestTimestampData.InMemory().getAllAsObjectArray();
  }

  private TestTimestampData timestamps;
  private PrefixTreeBlockMeta blockMeta;
  private LongEncoder encoder;
  private byte[] bytes;
  private TimestampDecoder decoder;

  public TestTimestampEncoder(TestTimestampData testTimestamps) throws IOException {
    this.timestamps = testTimestamps;
    this.blockMeta = new PrefixTreeBlockMeta();
    this.blockMeta.setNumMetaBytes(0);
    this.blockMeta.setNumRowBytes(0);
    this.blockMeta.setNumQualifierBytes(0);
    this.encoder = new LongEncoder();
    for (Long ts : testTimestamps.getInputs()) {
      encoder.add(ts);
    }
    encoder.compile();
    blockMeta.setTimestampFields(encoder);
    bytes = encoder.getByteArray();
    decoder = new TimestampDecoder();
    decoder.initOnBlock(blockMeta, bytes);
  }

  @Test
  public void testCompressorMinimum() {
    Assert.assertEquals(timestamps.getMinimum(), encoder.getMin());
  }

  @Test
  public void testCompressorRoundTrip() {
    long[] outputs = encoder.getSortedUniqueTimestamps();
    for (int i = 0; i < timestamps.getOutputs().size(); ++i) {
      long input = timestamps.getOutputs().get(i);
      long output = outputs[i];
      Assert.assertEquals(input, output);
    }
  }

  @Test
  public void testReaderMinimum() {
    Assert.assertEquals(timestamps.getMinimum(), decoder.getLong(0));
  }

  @Test
  public void testReaderRoundTrip() {
    for (int i = 0; i < timestamps.getOutputs().size(); ++i) {
      long input = timestamps.getOutputs().get(i);
      long output = decoder.getLong(i);
      Assert.assertEquals(input, output);
    }
  }
}
