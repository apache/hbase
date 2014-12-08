/**
 * Copyright The Apache Software Foundation
 *
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeCodec;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder.EncodedSeeker;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests scanning/seeking data with PrefixTree Encoding.
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestPrefixTreeEncoding {
  private static final Log LOG = LogFactory.getLog(TestPrefixTreeEncoding.class);
  private static final String CF = "EncodingTestCF";
  private static final byte[] CF_BYTES = Bytes.toBytes(CF);
  private static final int NUM_ROWS_PER_BATCH = 50;
  private static final int NUM_COLS_PER_ROW = 20;

  private int numBatchesWritten = 0;
  private ConcurrentSkipListSet<KeyValue> kvset = new ConcurrentSkipListSet<KeyValue>(
      KeyValue.COMPARATOR);

  private static boolean formatRowNum = false;
  
  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> paramList = new ArrayList<Object[]>();
    {
      paramList.add(new Object[] { false });
      paramList.add(new Object[] { true });
    }
    return paramList;
  }
  private final boolean includesTag;
  public TestPrefixTreeEncoding(boolean includesTag) {
    this.includesTag = includesTag;
  }
 
  @Before
  public void setUp() throws Exception {
    kvset.clear();
    formatRowNum = false;
  }

  @Test
  public void testSeekBeforeWithFixedData() throws Exception {
    formatRowNum = true;
    PrefixTreeCodec encoder = new PrefixTreeCodec();
    int batchId = numBatchesWritten++;
    ByteBuffer dataBuffer = generateFixedTestData(kvset, batchId, false, includesTag);
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(false)
                        .withIncludesMvcc(false)
                        .withIncludesTags(includesTag)
                        .withCompression(Algorithm.NONE).build();
    HFileBlockEncodingContext blkEncodingCtx = new HFileBlockDefaultEncodingContext(
        DataBlockEncoding.PREFIX_TREE, new byte[0], meta);
    encoder.encodeKeyValues(dataBuffer, blkEncodingCtx);
    EncodedSeeker seeker = encoder.createSeeker(KeyValue.COMPARATOR,
        encoder.newDataBlockDecodingContext(meta));
    byte[] onDiskBytes = blkEncodingCtx.getOnDiskBytesWithHeader();
    ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes, DataBlockEncoding.ID_SIZE,
        onDiskBytes.length - DataBlockEncoding.ID_SIZE);
    seeker.setCurrentBuffer(readBuffer);

    // Seek before the first keyvalue;
    KeyValue seekKey = KeyValue.createFirstDeleteFamilyOnRow(getRowKey(batchId, 0), CF_BYTES);
    seeker.seekToKeyInBlock(seekKey.getBuffer(), seekKey.getKeyOffset(), seekKey.getKeyLength(),
        true);
    assertEquals(null, seeker.getKeyValue());

    // Seek before the middle keyvalue;
    seekKey = KeyValue.createFirstDeleteFamilyOnRow(getRowKey(batchId, NUM_ROWS_PER_BATCH / 3),
        CF_BYTES);
    seeker.seekToKeyInBlock(seekKey.getBuffer(), seekKey.getKeyOffset(), seekKey.getKeyLength(),
        true);
    assertNotNull(seeker.getKeyValue());
    assertArrayEquals(getRowKey(batchId, NUM_ROWS_PER_BATCH / 3 - 1), seeker.getKeyValue().getRow());

    // Seek before the last keyvalue;
    seekKey = KeyValue.createFirstDeleteFamilyOnRow(Bytes.toBytes("zzzz"), CF_BYTES);
    seeker.seekToKeyInBlock(seekKey.getBuffer(), seekKey.getKeyOffset(), seekKey.getKeyLength(),
        true);
    assertNotNull(seeker.getKeyValue());
    assertArrayEquals(getRowKey(batchId, NUM_ROWS_PER_BATCH - 1), seeker.getKeyValue().getRow());
  }

  @Test
  public void testScanWithRandomData() throws Exception {
    PrefixTreeCodec encoder = new PrefixTreeCodec();
    ByteBuffer dataBuffer = generateRandomTestData(kvset, numBatchesWritten++, includesTag);
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(false)
                        .withIncludesMvcc(false)
                        .withIncludesTags(includesTag)
                        .withCompression(Algorithm.NONE)
                        .build();
    HFileBlockEncodingContext blkEncodingCtx = new HFileBlockDefaultEncodingContext(
        DataBlockEncoding.PREFIX_TREE, new byte[0], meta);
    encoder.encodeKeyValues(dataBuffer, blkEncodingCtx);
    EncodedSeeker seeker = encoder.createSeeker(KeyValue.COMPARATOR,
        encoder.newDataBlockDecodingContext(meta));
    byte[] onDiskBytes = blkEncodingCtx.getOnDiskBytesWithHeader();
    ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes, DataBlockEncoding.ID_SIZE,
        onDiskBytes.length - DataBlockEncoding.ID_SIZE);
    seeker.setCurrentBuffer(readBuffer);
    KeyValue previousKV = null;
    do {
      KeyValue currentKV = seeker.getKeyValue();
      System.out.println(currentKV);
      if (previousKV != null && KeyValue.COMPARATOR.compare(currentKV, previousKV) < 0) {
        dumpInputKVSet();
        fail("Current kv " + currentKV + " is smaller than previous keyvalue " + previousKV);
      }
      if (!includesTag) {
        assertFalse(currentKV.getTagsLengthUnsigned() > 0);
      } else {
        Assert.assertTrue(currentKV.getTagsLengthUnsigned() > 0);
      }
      previousKV = currentKV;
    } while (seeker.next());
  }

  @Test
  public void testSeekWithRandomData() throws Exception {
    PrefixTreeCodec encoder = new PrefixTreeCodec();
    int batchId = numBatchesWritten++;
    ByteBuffer dataBuffer = generateRandomTestData(kvset, batchId, includesTag);
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(false)
                        .withIncludesMvcc(false)
                        .withIncludesTags(includesTag)
                        .withCompression(Algorithm.NONE)
                        .build();
    HFileBlockEncodingContext blkEncodingCtx = new HFileBlockDefaultEncodingContext(
        DataBlockEncoding.PREFIX_TREE, new byte[0], meta);
    encoder.encodeKeyValues(dataBuffer, blkEncodingCtx);
    EncodedSeeker seeker = encoder.createSeeker(KeyValue.COMPARATOR,
        encoder.newDataBlockDecodingContext(meta));
    byte[] onDiskBytes = blkEncodingCtx.getOnDiskBytesWithHeader();
    ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes, DataBlockEncoding.ID_SIZE,
        onDiskBytes.length - DataBlockEncoding.ID_SIZE);
    verifySeeking(seeker, readBuffer, batchId);
  }

  @Test
  public void testSeekWithFixedData() throws Exception {
    PrefixTreeCodec encoder = new PrefixTreeCodec();
    int batchId = numBatchesWritten++;
    ByteBuffer dataBuffer = generateFixedTestData(kvset, batchId, includesTag);
    HFileContext meta = new HFileContextBuilder()
                        .withHBaseCheckSum(false)
                        .withIncludesMvcc(false)
                        .withIncludesTags(includesTag)
                        .withCompression(Algorithm.NONE)
                        .build();
    HFileBlockEncodingContext blkEncodingCtx = new HFileBlockDefaultEncodingContext(
        DataBlockEncoding.PREFIX_TREE, new byte[0], meta);
    encoder.encodeKeyValues(dataBuffer, blkEncodingCtx);
    EncodedSeeker seeker = encoder.createSeeker(KeyValue.COMPARATOR,
        encoder.newDataBlockDecodingContext(meta));
    byte[] onDiskBytes = blkEncodingCtx.getOnDiskBytesWithHeader();
    ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes, DataBlockEncoding.ID_SIZE,
        onDiskBytes.length - DataBlockEncoding.ID_SIZE);
    verifySeeking(seeker, readBuffer, batchId);
  }
  
  private void verifySeeking(EncodedSeeker encodeSeeker,
      ByteBuffer encodedData, int batchId) {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    for (int i = 0; i < NUM_ROWS_PER_BATCH; ++i) {
      kvList.clear();
      encodeSeeker.setCurrentBuffer(encodedData);
      KeyValue firstOnRow = KeyValue.createFirstOnRow(getRowKey(batchId, i));
      encodeSeeker.seekToKeyInBlock(firstOnRow.getBuffer(),
          firstOnRow.getKeyOffset(), firstOnRow.getKeyLength(), false);
      boolean hasMoreOfEncodeScanner = encodeSeeker.next();
      CollectionBackedScanner collectionScanner = new CollectionBackedScanner(
          this.kvset);
      boolean hasMoreOfCollectionScanner = collectionScanner.seek(firstOnRow);
      if (hasMoreOfEncodeScanner != hasMoreOfCollectionScanner) {
        dumpInputKVSet();
        fail("Get error result after seeking " + firstOnRow);
      }
      if (hasMoreOfEncodeScanner) {
        if (KeyValue.COMPARATOR.compare(encodeSeeker.getKeyValue(),
            collectionScanner.peek()) != 0) {
          dumpInputKVSet();
          fail("Expected " + collectionScanner.peek() + " actual "
              + encodeSeeker.getKeyValue() + ", after seeking " + firstOnRow);
        }
      }
    }
  }

  private void dumpInputKVSet() {
    LOG.info("Dumping input keyvalue set in error case:");
    for (KeyValue kv : kvset) {
      System.out.println(kv);
    }
  }

  private static ByteBuffer generateFixedTestData(ConcurrentSkipListSet<KeyValue> kvset,
      int batchId, boolean useTags) throws Exception {
    return generateFixedTestData(kvset, batchId, true, useTags);
  }

  private static ByteBuffer generateFixedTestData(ConcurrentSkipListSet<KeyValue> kvset,
      int batchId, boolean partial, boolean useTags) throws Exception {
    ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
    DataOutputStream userDataStream = new DataOutputStream(baosInMemory);
    for (int i = 0; i < NUM_ROWS_PER_BATCH; ++i) {
      if (partial && i / 10 % 2 == 1)
        continue;
      for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
        if (!useTags) {
          KeyValue kv = new KeyValue(getRowKey(batchId, i), CF_BYTES, getQualifier(j), getValue(
              batchId, i, j));
          kvset.add(kv);
        } else {
          KeyValue kv = new KeyValue(getRowKey(batchId, i), CF_BYTES, getQualifier(j), 0l,
              getValue(batchId, i, j), new Tag[] { new Tag((byte) 1, "metaValue1") });
          kvset.add(kv);
        }
      }
    }
    for (KeyValue kv : kvset) {
      userDataStream.writeInt(kv.getKeyLength());
      userDataStream.writeInt(kv.getValueLength());
      userDataStream.write(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
      userDataStream.write(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
      if (useTags) {
        userDataStream.writeShort(kv.getTagsLengthUnsigned());
        userDataStream.write(kv.getBuffer(), kv.getValueOffset() + kv.getValueLength()
            + Bytes.SIZEOF_SHORT, kv.getTagsLengthUnsigned());
      }
    }
    return ByteBuffer.wrap(baosInMemory.toByteArray());
  }

  private static ByteBuffer generateRandomTestData(ConcurrentSkipListSet<KeyValue> kvset,
      int batchId, boolean useTags) throws Exception {
    ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
    DataOutputStream userDataStream = new DataOutputStream(baosInMemory);
    Random random = new Random();
    for (int i = 0; i < NUM_ROWS_PER_BATCH; ++i) {
      if (random.nextInt(100) < 50)
        continue;
      for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
        if (random.nextInt(100) < 50)
          continue;
        if (!useTags) {
          KeyValue kv = new KeyValue(getRowKey(batchId, i), CF_BYTES, getQualifier(j), getValue(
              batchId, i, j));
          kvset.add(kv);
        } else {
          KeyValue kv = new KeyValue(getRowKey(batchId, i), CF_BYTES, getQualifier(j), 0l,
              getValue(batchId, i, j), new Tag[] { new Tag((byte) 1, "metaValue1") });
          kvset.add(kv);
        }
      }
    }

    for (KeyValue kv : kvset) {
      userDataStream.writeInt(kv.getKeyLength());
      userDataStream.writeInt(kv.getValueLength());
      userDataStream.write(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
      userDataStream.write(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
      if (useTags) {
        userDataStream.writeShort(kv.getTagsLengthUnsigned());
        userDataStream.write(kv.getBuffer(), kv.getValueOffset() + kv.getValueLength()
            + Bytes.SIZEOF_SHORT, kv.getTagsLengthUnsigned());
      }
    }
    return ByteBuffer.wrap(baosInMemory.toByteArray());
  }

  private static byte[] getRowKey(int batchId, int i) {
    return Bytes
        .toBytes("batch" + batchId + "_row" + (formatRowNum ? String.format("%04d", i) : i));
  }

  private static byte[] getQualifier(int j) {
    return Bytes.toBytes("colfdfafhfhsdfhsdfh" + j);
  }

  private static byte[] getValue(int batchId, int i, int j) {
    return Bytes.toBytes("value_for_" + Bytes.toString(getRowKey(batchId, i)) + "_col" + j);
  }

}
