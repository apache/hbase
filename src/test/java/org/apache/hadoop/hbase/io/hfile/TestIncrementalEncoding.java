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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestIncrementalEncoding {
  
  private static final Log LOG = LogFactory.getLog(TestIncrementalEncoding.class);

  private static final int BLOCK_SIZE = 1024;
  private static final int KVTYPES = 4;
  private static final byte[] FAMILY = Bytes.toBytes("family");

  public void testEncoding(DataBlockEncoding dataEncoding, boolean includeMemstoreTS,
      int kvType) throws IOException {
    LOG.info("encoding=" + dataEncoding + ", includeMemstoreTS=" + includeMemstoreTS + ", " +
      "kvType=" + kvType);
    HFileDataBlockEncoder blockEncoder = new HFileDataBlockEncoderImpl(dataEncoding);
    HFileBlock.Writer writerEncoded = new HFileBlock.Writer(null, blockEncoder,
        includeMemstoreTS);
    HFileBlock.Writer writerUnencoded = new HFileBlock.Writer(null,
        NoOpDataBlockEncoder.INSTANCE, includeMemstoreTS);
    writerEncoded.startWriting(BlockType.DATA);
    writerUnencoded.startWriting(BlockType.DATA);

    // Fill block with data
    long time = 1 << 10;
    while (writerEncoded.blockSizeWritten() < BLOCK_SIZE) {
      KeyValue kv;
      switch (kvType) {
        case 3:
          kv = new KeyValue(Bytes.toBytes(time), FAMILY,
              Bytes.toBytes(time), time, Bytes.toBytes(time));
          break;
        case 2:
          kv = new KeyValue(Bytes.toBytes("row"), FAMILY,
              Bytes.toBytes("qf" + time), 0, Bytes.toBytes("V"));
          break;
        case 1:
          kv = new KeyValue(Bytes.toBytes("row"), FAMILY,
              Bytes.toBytes("qf" + time), time, Bytes.toBytes("V" + time));
          break;
        default:
          kv = new KeyValue(Bytes.toBytes("row" + time), FAMILY,
              Bytes.toBytes("qf"), 0, Bytes.toBytes("Value"));
      }
      time++;
      appendEncoded(kv, writerEncoded);
      appendEncoded(kv, writerUnencoded);
    }

    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    writerEncoded.writeHeaderAndData(new DataOutputStream(encoded));

    ByteArrayOutputStream unencoded = new ByteArrayOutputStream();
    writerUnencoded.writeHeaderAndData(new DataOutputStream(unencoded));

    ByteArrayOutputStream encodedAgain = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(encodedAgain);
    int bytesToSkip = HFileBlock.HEADER_SIZE;
    ByteBuffer unencodedWithoutHeader = ByteBuffer.wrap(unencoded.toByteArray(), bytesToSkip,
        unencoded.size() - bytesToSkip).slice();
    dataEncoding.getEncoder().encodeKeyValues(dataOut,
        unencodedWithoutHeader, includeMemstoreTS);

    assertEquals(encodedAgain.size() + HFileBlock.HEADER_SIZE +
        dataEncoding.encodingIdSize(), encoded.size());

    byte[] en = encoded.toByteArray();
    byte[] en2 = encodedAgain.toByteArray();
    int shift = HFileBlock.HEADER_SIZE + dataEncoding.encodingIdSize();
    for (int i = 0; i < encodedAgain.size(); i++) {
      assertEquals("Byte" + i, en2[i], en[i + shift]);
    }
  }

  private void testOneEncodingWithAllKVTypes(DataBlockEncoding blockEncoding,
      boolean includeMemstoreTS) throws IOException {
    for (int i = 0; i < KVTYPES; i++) {
      testEncoding(blockEncoding, includeMemstoreTS, i);
    }
  }

  @Test
  public void testAllEncodings() throws IOException {
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      for (boolean includeMemstoreTS : HConstants.BOOLEAN_VALUES) {
        testOneEncodingWithAllKVTypes(encoding, includeMemstoreTS);
      }
    }
  }

  public void appendEncoded(final KeyValue kv, HFileBlock.Writer writer)
      throws IOException {
    writer.appendEncodedKV(kv.getMemstoreTS(), kv.getBuffer(),
        kv.getKeyOffset(), kv.getKeyLength(), kv.getBuffer(),
        kv.getValueOffset(), kv.getValueLength());
  }
}
