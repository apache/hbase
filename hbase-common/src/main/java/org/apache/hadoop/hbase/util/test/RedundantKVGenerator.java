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
package org.apache.hadoop.hbase.util.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.OffheapKeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.primitives.Bytes;

/**
 * Generate list of key values which are very useful to test data block encoding
 * and compression.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="RV_ABSOLUTE_VALUE_OF_RANDOM_INT",
    justification="Should probably fix")
@InterfaceAudience.Private
public class RedundantKVGenerator {
  // row settings
  static byte[] DEFAULT_COMMON_PREFIX = new byte[0];
  static int DEFAULT_NUMBER_OF_ROW_PREFIXES = 10;
  static int DEFAULT_AVERAGE_PREFIX_LENGTH = 6;
  static int DEFAULT_PREFIX_LENGTH_VARIANCE = 3;
  static int DEFAULT_AVERAGE_SUFFIX_LENGTH = 3;
  static int DEFAULT_SUFFIX_LENGTH_VARIANCE = 3;
  static int DEFAULT_NUMBER_OF_ROW = 500;

  // qualifier
  static float DEFAULT_CHANCE_FOR_SAME_QUALIFIER = 0.5f;
  static float DEFAULT_CHANCE_FOR_SIMILIAR_QUALIFIER = 0.4f;
  static int DEFAULT_AVERAGE_QUALIFIER_LENGTH = 9;
  static int DEFAULT_QUALIFIER_LENGTH_VARIANCE = 3;

  static int DEFAULT_COLUMN_FAMILY_LENGTH = 9;
  static int DEFAULT_VALUE_LENGTH = 8;
  static float DEFAULT_CHANCE_FOR_ZERO_VALUE = 0.5f;

  static int DEFAULT_BASE_TIMESTAMP_DIVIDE = 1000000;
  static int DEFAULT_TIMESTAMP_DIFF_SIZE = 100000000;

  /**
   * Default constructor, assumes all parameters from class constants.
   */
  public RedundantKVGenerator() {
    this(new Random(42L),
        DEFAULT_NUMBER_OF_ROW_PREFIXES,
        DEFAULT_AVERAGE_PREFIX_LENGTH,
        DEFAULT_PREFIX_LENGTH_VARIANCE,
        DEFAULT_AVERAGE_SUFFIX_LENGTH,
        DEFAULT_SUFFIX_LENGTH_VARIANCE,
        DEFAULT_NUMBER_OF_ROW,

        DEFAULT_CHANCE_FOR_SAME_QUALIFIER,
        DEFAULT_CHANCE_FOR_SIMILIAR_QUALIFIER,
        DEFAULT_AVERAGE_QUALIFIER_LENGTH,
        DEFAULT_QUALIFIER_LENGTH_VARIANCE,

        DEFAULT_COLUMN_FAMILY_LENGTH,
        DEFAULT_VALUE_LENGTH,
        DEFAULT_CHANCE_FOR_ZERO_VALUE,

        DEFAULT_BASE_TIMESTAMP_DIVIDE,
        DEFAULT_TIMESTAMP_DIFF_SIZE
    );
  }


  /**
   * Various configuration options for generating key values
   * @param randomizer pick things by random
   */
  public RedundantKVGenerator(Random randomizer,
      int numberOfRowPrefixes,
      int averagePrefixLength,
      int prefixLengthVariance,
      int averageSuffixLength,
      int suffixLengthVariance,
      int numberOfRows,

      float chanceForSameQualifier,
      float chanceForSimiliarQualifier,
      int averageQualifierLength,
      int qualifierLengthVariance,

      int columnFamilyLength,
      int valueLength,
      float chanceForZeroValue,

      int baseTimestampDivide,
      int timestampDiffSize
      ) {
    this.randomizer = randomizer;

    this.commonPrefix = DEFAULT_COMMON_PREFIX;
    this.numberOfRowPrefixes = numberOfRowPrefixes;
    this.averagePrefixLength = averagePrefixLength;
    this.prefixLengthVariance = prefixLengthVariance;
    this.averageSuffixLength = averageSuffixLength;
    this.suffixLengthVariance = suffixLengthVariance;
    this.numberOfRows = numberOfRows;

    this.chanceForSameQualifier = chanceForSameQualifier;
    this.chanceForSimilarQualifier = chanceForSimiliarQualifier;
    this.averageQualifierLength = averageQualifierLength;
    this.qualifierLengthVariance = qualifierLengthVariance;

    this.columnFamilyLength = columnFamilyLength;
    this.valueLength = valueLength;
    this.chanceForZeroValue = chanceForZeroValue;

    this.baseTimestampDivide = baseTimestampDivide;
    this.timestampDiffSize = timestampDiffSize;
  }

  /** Used to generate dataset */
  private Random randomizer;

  // row settings
  private byte[] commonPrefix;//global prefix before rowPrefixes
  private int numberOfRowPrefixes;
  private int averagePrefixLength = 6;
  private int prefixLengthVariance = 3;
  private int averageSuffixLength = 3;
  private int suffixLengthVariance = 3;
  private int numberOfRows = 500;

  //family
  private byte[] family;

  // qualifier
  private float chanceForSameQualifier = 0.5f;
  private float chanceForSimilarQualifier = 0.4f;
  private int averageQualifierLength = 9;
  private int qualifierLengthVariance = 3;

  private int columnFamilyLength = 9;
  private int valueLength = 8;
  private float chanceForZeroValue = 0.5f;

  private int baseTimestampDivide = 1000000;
  private int timestampDiffSize = 100000000;

  private List<byte[]> generateRows() {
    // generate prefixes
    List<byte[]> prefixes = new ArrayList<byte[]>();
    prefixes.add(new byte[0]);
    for (int i = 1; i < numberOfRowPrefixes; ++i) {
      int prefixLength = averagePrefixLength;
      prefixLength += randomizer.nextInt(2 * prefixLengthVariance + 1) -
          prefixLengthVariance;
      byte[] newPrefix = new byte[prefixLength];
      randomizer.nextBytes(newPrefix);
      byte[] newPrefixWithCommon = newPrefix;
      prefixes.add(newPrefixWithCommon);
    }

    // generate rest of the row
    List<byte[]> rows = new ArrayList<byte[]>();
    for (int i = 0; i < numberOfRows; ++i) {
      int suffixLength = averageSuffixLength;
      suffixLength += randomizer.nextInt(2 * suffixLengthVariance + 1) -
          suffixLengthVariance;
      int randomPrefix = randomizer.nextInt(prefixes.size());
      byte[] row = new byte[prefixes.get(randomPrefix).length +
                            suffixLength];
      byte[] rowWithCommonPrefix = Bytes.concat(commonPrefix, row);
      rows.add(rowWithCommonPrefix);
    }

    return rows;
  }

  /**
   * Generate test data useful to test encoders.
   * @param howMany How many Key values should be generated.
   * @return sorted list of key values
   */
  public List<KeyValue> generateTestKeyValues(int howMany) {
    return generateTestKeyValues(howMany, false);
  }
  /**
   * Generate test data useful to test encoders.
   * @param howMany How many Key values should be generated.
   * @return sorted list of key values
   */
  public List<KeyValue> generateTestKeyValues(int howMany, boolean useTags) {
    List<KeyValue> result = new ArrayList<KeyValue>();

    List<byte[]> rows = generateRows();
    Map<Integer, List<byte[]>> rowsToQualifier = new HashMap<Integer, List<byte[]>>();

    if(family==null){
      family = new byte[columnFamilyLength];
      randomizer.nextBytes(family);
    }

    long baseTimestamp = Math.abs(randomizer.nextInt()) / baseTimestampDivide;

    byte[] value = new byte[valueLength];

    for (int i = 0; i < howMany; ++i) {
      long timestamp = baseTimestamp;
      if(timestampDiffSize > 0){
        timestamp += randomizer.nextInt(timestampDiffSize);
      }
      Integer rowId = randomizer.nextInt(rows.size());
      byte[] row = rows.get(rowId);

      // generate qualifier, sometimes it is same, sometimes similar,
      // occasionally completely different
      byte[] qualifier;
      float qualifierChance = randomizer.nextFloat();
      if (!rowsToQualifier.containsKey(rowId)
          || qualifierChance > chanceForSameQualifier + chanceForSimilarQualifier) {
        int qualifierLength = averageQualifierLength;
        qualifierLength += randomizer.nextInt(2 * qualifierLengthVariance + 1)
            - qualifierLengthVariance;
        qualifier = new byte[qualifierLength];
        randomizer.nextBytes(qualifier);

        // add it to map
        if (!rowsToQualifier.containsKey(rowId)) {
          rowsToQualifier.put(rowId, new ArrayList<byte[]>());
        }
        rowsToQualifier.get(rowId).add(qualifier);
      } else if (qualifierChance > chanceForSameQualifier) {
        // similar qualifier
        List<byte[]> previousQualifiers = rowsToQualifier.get(rowId);
        byte[] originalQualifier = previousQualifiers.get(randomizer.nextInt(previousQualifiers
            .size()));

        qualifier = new byte[originalQualifier.length];
        int commonPrefix = randomizer.nextInt(qualifier.length);
        System.arraycopy(originalQualifier, 0, qualifier, 0, commonPrefix);
        for (int j = commonPrefix; j < qualifier.length; ++j) {
          qualifier[j] = (byte) (randomizer.nextInt() & 0xff);
        }

        rowsToQualifier.get(rowId).add(qualifier);
      } else {
        // same qualifier
        List<byte[]> previousQualifiers = rowsToQualifier.get(rowId);
        qualifier = previousQualifiers.get(randomizer.nextInt(previousQualifiers.size()));
      }

      if (randomizer.nextFloat() < chanceForZeroValue) {
        for (int j = 0; j < value.length; ++j) {
          value[j] = (byte) 0;
        }
      } else {
        randomizer.nextBytes(value);
      }

      if (useTags) {
        result.add(new KeyValue(row, family, qualifier, timestamp, value, new Tag[] { new Tag(
            (byte) 1, "value1") }));
      } else {
        result.add(new KeyValue(row, family, qualifier, timestamp, value));
      }
    }

    Collections.sort(result, CellComparator.COMPARATOR);

    return result;
  }

  /**
   * Generate test data useful to test encoders.
   * @param howMany How many Key values should be generated.
   * @return sorted list of key values
   */
  public List<Cell> generateTestExtendedOffheapKeyValues(int howMany, boolean useTags) {
    List<Cell> result = new ArrayList<Cell>();
    List<byte[]> rows = generateRows();
    Map<Integer, List<byte[]>> rowsToQualifier = new HashMap<Integer, List<byte[]>>();

    if (family == null) {
      family = new byte[columnFamilyLength];
      randomizer.nextBytes(family);
    }

    long baseTimestamp = Math.abs(randomizer.nextInt()) / baseTimestampDivide;

    byte[] value = new byte[valueLength];

    for (int i = 0; i < howMany; ++i) {
      long timestamp = baseTimestamp;
      if(timestampDiffSize > 0){
        timestamp += randomizer.nextInt(timestampDiffSize);
      }
      Integer rowId = randomizer.nextInt(rows.size());
      byte[] row = rows.get(rowId);

      // generate qualifier, sometimes it is same, sometimes similar,
      // occasionally completely different
      byte[] qualifier;
      float qualifierChance = randomizer.nextFloat();
      if (!rowsToQualifier.containsKey(rowId)
          || qualifierChance > chanceForSameQualifier + chanceForSimilarQualifier) {
        int qualifierLength = averageQualifierLength;
        qualifierLength += randomizer.nextInt(2 * qualifierLengthVariance + 1)
            - qualifierLengthVariance;
        qualifier = new byte[qualifierLength];
        randomizer.nextBytes(qualifier);

        // add it to map
        if (!rowsToQualifier.containsKey(rowId)) {
          rowsToQualifier.put(rowId, new ArrayList<byte[]>());
        }
        rowsToQualifier.get(rowId).add(qualifier);
      } else if (qualifierChance > chanceForSameQualifier) {
        // similar qualifier
        List<byte[]> previousQualifiers = rowsToQualifier.get(rowId);
        byte[] originalQualifier = previousQualifiers.get(randomizer.nextInt(previousQualifiers
            .size()));

        qualifier = new byte[originalQualifier.length];
        int commonPrefix = randomizer.nextInt(qualifier.length);
        System.arraycopy(originalQualifier, 0, qualifier, 0, commonPrefix);
        for (int j = commonPrefix; j < qualifier.length; ++j) {
          qualifier[j] = (byte) (randomizer.nextInt() & 0xff);
        }

        rowsToQualifier.get(rowId).add(qualifier);
      } else {
        // same qualifier
        List<byte[]> previousQualifiers = rowsToQualifier.get(rowId);
        qualifier = previousQualifiers.get(randomizer.nextInt(previousQualifiers.size()));
      }

      if (randomizer.nextFloat() < chanceForZeroValue) {
        for (int j = 0; j < value.length; ++j) {
          value[j] = (byte) 0;
        }
      } else {
        randomizer.nextBytes(value);
      }
      if (useTags) {
        KeyValue keyValue = new KeyValue(row, family, qualifier, timestamp, value,
            new Tag[] { new Tag((byte) 1, "value1") });
        ByteBuffer offheapKVBB = ByteBuffer.allocateDirect(keyValue.getLength());
        ByteBufferUtils.copyFromArrayToBuffer(offheapKVBB, keyValue.getBuffer(),
          keyValue.getOffset(), keyValue.getLength());
        OffheapKeyValue offheapKV =
            new ExtendedOffheapKeyValue(offheapKVBB, 0, keyValue.getLength(), true, 0);
        result.add(offheapKV);
      } else {
        KeyValue keyValue = new KeyValue(row, family, qualifier, timestamp, value);
        ByteBuffer offheapKVBB = ByteBuffer.allocateDirect(keyValue.getLength());
        ByteBufferUtils.copyFromArrayToBuffer(offheapKVBB, keyValue.getBuffer(),
          keyValue.getOffset(), keyValue.getLength());
        OffheapKeyValue offheapKV =
            new ExtendedOffheapKeyValue(offheapKVBB, 0, keyValue.getLength(), false, 0);
        result.add(offheapKV);
      }
    }

    Collections.sort(result, CellComparator.COMPARATOR);

    return result;
  }

  static class ExtendedOffheapKeyValue extends OffheapKeyValue {
    public ExtendedOffheapKeyValue(ByteBuffer buf, int offset, int length, boolean hasTags,
        long seqId) {
      super(buf, offset, length, hasTags, seqId);
    }

    @Override
    public byte[] getRowArray() {
      throw new IllegalArgumentException("getRowArray operation is not allowed");
    }

    @Override
    public int getRowOffset() {
      throw new IllegalArgumentException("getRowOffset operation is not allowed");
    }

    @Override
    public byte[] getFamilyArray() {
      throw new IllegalArgumentException("getFamilyArray operation is not allowed");
    }

    @Override
    public int getFamilyOffset() {
      throw new IllegalArgumentException("getFamilyOffset operation is not allowed");
    }

    @Override
    public byte[] getQualifierArray() {
      throw new IllegalArgumentException("getQualifierArray operation is not allowed");
    }

    @Override
    public int getQualifierOffset() {
      throw new IllegalArgumentException("getQualifierOffset operation is not allowed");
    }

    @Override
    public byte[] getValueArray() {
      throw new IllegalArgumentException("getValueArray operation is not allowed");
    }

    @Override
    public int getValueOffset() {
      throw new IllegalArgumentException("getValueOffset operation is not allowed");
    }

    @Override
    public byte[] getTagsArray() {
      throw new IllegalArgumentException("getTagsArray operation is not allowed");
    }

    @Override
    public int getTagsOffset() {
      throw new IllegalArgumentException("getTagsOffset operation is not allowed");
    }
  }

  /**
   * Convert list of KeyValues to byte buffer.
   * @param keyValues list of KeyValues to be converted.
   * @return buffer with content from key values
   */
  public static ByteBuffer convertKvToByteBuffer(List<KeyValue> keyValues,
      boolean includesMemstoreTS) {
    int totalSize = 0;
    for (KeyValue kv : keyValues) {
      totalSize += kv.getLength();
      if (includesMemstoreTS) {
        totalSize += WritableUtils.getVIntSize(kv.getSequenceId());
      }
    }

    ByteBuffer result = ByteBuffer.allocate(totalSize);
    for (KeyValue kv : keyValues) {
      result.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
      if (includesMemstoreTS) {
        ByteBufferUtils.writeVLong(result, kv.getSequenceId());
      }
    }
    return result;
  }
  
  
  /************************ get/set ***********************************/
  
  public RedundantKVGenerator setCommonPrefix(byte[] prefix){
    this.commonPrefix = prefix;
    return this;
  }

  public RedundantKVGenerator setRandomizer(Random randomizer) {
    this.randomizer = randomizer;
    return this;
  }

  public RedundantKVGenerator setNumberOfRowPrefixes(int numberOfRowPrefixes) {
    this.numberOfRowPrefixes = numberOfRowPrefixes;
    return this;
  }

  public RedundantKVGenerator setAveragePrefixLength(int averagePrefixLength) {
    this.averagePrefixLength = averagePrefixLength;
    return this;
  }

  public RedundantKVGenerator setPrefixLengthVariance(int prefixLengthVariance) {
    this.prefixLengthVariance = prefixLengthVariance;
    return this;
  }

  public RedundantKVGenerator setAverageSuffixLength(int averageSuffixLength) {
    this.averageSuffixLength = averageSuffixLength;
    return this;
  }

  public RedundantKVGenerator setSuffixLengthVariance(int suffixLengthVariance) {
    this.suffixLengthVariance = suffixLengthVariance;
    return this;
  }

  public RedundantKVGenerator setNumberOfRows(int numberOfRows) {
    this.numberOfRows = numberOfRows;
    return this;
  }

  public RedundantKVGenerator setChanceForSameQualifier(float chanceForSameQualifier) {
    this.chanceForSameQualifier = chanceForSameQualifier;
    return this;
  }

  public RedundantKVGenerator setChanceForSimilarQualifier(float chanceForSimiliarQualifier) {
    this.chanceForSimilarQualifier = chanceForSimiliarQualifier;
    return this;
  }

  public RedundantKVGenerator setAverageQualifierLength(int averageQualifierLength) {
    this.averageQualifierLength = averageQualifierLength;
    return this;
  }

  public RedundantKVGenerator setQualifierLengthVariance(int qualifierLengthVariance) {
    this.qualifierLengthVariance = qualifierLengthVariance;
    return this;
  }

  public RedundantKVGenerator setColumnFamilyLength(int columnFamilyLength) {
    this.columnFamilyLength = columnFamilyLength;
    return this;
  }

  public RedundantKVGenerator setFamily(byte[] family) {
    this.family = family;
    this.columnFamilyLength = family.length;
    return this;
  }

  public RedundantKVGenerator setValueLength(int valueLength) {
    this.valueLength = valueLength;
    return this;
  }

  public RedundantKVGenerator setChanceForZeroValue(float chanceForZeroValue) {
    this.chanceForZeroValue = chanceForZeroValue;
    return this;
  }

  public RedundantKVGenerator setBaseTimestampDivide(int baseTimestampDivide) {
    this.baseTimestampDivide = baseTimestampDivide;
    return this;
  }

  public RedundantKVGenerator setTimestampDiffSize(int timestampDiffSize) {
    this.timestampDiffSize = timestampDiffSize;
    return this;
  }
  
}
