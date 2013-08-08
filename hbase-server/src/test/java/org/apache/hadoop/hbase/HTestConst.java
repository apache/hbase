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
package org.apache.hadoop.hbase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Similar to {@link HConstants} but for tests. Also provides some simple
 * static utility functions to generate test data.
 */
public class HTestConst {

  private HTestConst() {
  }

  public static final String DEFAULT_TABLE_STR = "MyTestTable";
  public static final byte[] DEFAULT_TABLE_BYTES = Bytes.toBytes(DEFAULT_TABLE_STR);
  public static final TableName DEFAULT_TABLE =
      TableName.valueOf(DEFAULT_TABLE_BYTES);

  public static final String DEFAULT_CF_STR = "MyDefaultCF";
  public static final byte[] DEFAULT_CF_BYTES = Bytes.toBytes(DEFAULT_CF_STR);

  public static final Set<String> DEFAULT_CF_STR_SET =
      Collections.unmodifiableSet(new HashSet<String>(
          Arrays.asList(new String[] { DEFAULT_CF_STR })));

  public static final String DEFAULT_ROW_STR = "MyTestRow";
  public static final byte[] DEFAULT_ROW_BYTES = Bytes.toBytes(DEFAULT_ROW_STR);

  public static final String DEFAULT_QUALIFIER_STR = "MyColumnQualifier";
  public static final byte[] DEFAULT_QUALIFIER_BYTES = Bytes.toBytes(DEFAULT_QUALIFIER_STR);

  public static String DEFAULT_VALUE_STR = "MyTestValue";
  public static byte[] DEFAULT_VALUE_BYTES = Bytes.toBytes(DEFAULT_VALUE_STR);

  /**
   * Generate the given number of unique byte sequences by appending numeric
   * suffixes (ASCII representations of decimal numbers).
   */
  public static byte[][] makeNAscii(byte[] base, int n) {
    byte [][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      byte[] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

}
