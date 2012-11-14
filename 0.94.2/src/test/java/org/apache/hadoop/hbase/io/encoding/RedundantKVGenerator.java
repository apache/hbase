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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.io.WritableUtils;

/**
 * Generate list of key values which are very useful to test data block encoding
 * and compression.
 */
public class RedundantKVGenerator {
  // row settings
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

    this.numberOfRowPrefixes = numberOfRowPrefixes;
    this.averagePrefixLength = averagePrefixLength;
    this.prefixLengthVariance = prefixLengthVariance;
    this.averageSuffixLength = averageSuffixLength;
    this.suffixLengthVariance = suffixLengthVariance;
    this.numberOfRows = numberOfRows;

    this.chanceForSameQualifier = chanceForSameQualifier;
    this.chanceForSimiliarQualifier = chanceForSimiliarQualifier;
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
  private int numberOfRowPrefixes;
  private int averagePrefixLength = 6;
  private int prefixLengthVariance = 3;
  private int averageSuffixLength = 3;
  private int suffixLengthVariance = 3;
  private int numberOfRows = 500;

  // qualifier
  private float chanceForSameQualifier = 0.5f;
  private float chanceForSimiliarQualifier = 0.4f;
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
      prefixes.add(newPrefix);
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
      rows.add(row);
    }

    return rows;
  }

  /**
   * Generate test data useful to test encoders.
   * @param howMany How many Key values should be generated.
   * @return sorted list of key values
   */
  public List<KeyValue> generateTestKeyValues(int howMany) {
    List<KeyValue> result = new ArrayList<KeyValue>();

    List<byte[]> rows = generateRows();
    Map<Integer, List<byte[]>> rowsToQualifier =
        new HashMap<Integer, List<byte[]>>();

    byte[] family = new byte[columnFamilyLength];
    randomizer.nextBytes(family);

    long baseTimestamp = Math.abs(randomizer.nextLong()) /
        baseTimestampDivide;

    byte[] value = new byte[valueLength];

    for (int i = 0; i < howMany; ++i) {
      long timestamp = baseTimestamp + randomizer.nextInt(
          timestampDiffSize);
      Integer rowId = randomizer.nextInt(rows.size());
      byte[] row = rows.get(rowId);

      // generate qualifier, sometimes it is same, sometimes similar,
      // occasionally completely different
      byte[] qualifier;
      float qualifierChance = randomizer.nextFloat();
      if (!rowsToQualifier.containsKey(rowId) ||
          qualifierChance > chanceForSameQualifier +
          chanceForSimiliarQualifier) {
        int qualifierLength = averageQualifierLength;
        qualifierLength +=
            randomizer.nextInt(2 * qualifierLengthVariance + 1) -
            qualifierLengthVariance;
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
        byte[] originalQualifier = previousQualifiers.get(
            randomizer.nextInt(previousQualifiers.size()));

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
        qualifier = previousQualifiers.get(
            randomizer.nextInt(previousQualifiers.size()));
      }

      if (randomizer.nextFloat() < chanceForZeroValue) {
        for (int j = 0; j < value.length; ++j) {
          value[j] = (byte) 0;
        }
      } else {
        randomizer.nextBytes(value);
      }

      result.add(new KeyValue(row, family, qualifier, timestamp, value));
    }

    Collections.sort(result, KeyValue.COMPARATOR);

    return result;
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
        totalSize += WritableUtils.getVIntSize(kv.getMemstoreTS());
      }
    }

    ByteBuffer result = ByteBuffer.allocate(totalSize);
    for (KeyValue kv : keyValues) {
      result.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
      if (includesMemstoreTS) {
        ByteBufferUtils.writeVLong(result, kv.getMemstoreTS());
      }
    }

    return result;
  }

}
