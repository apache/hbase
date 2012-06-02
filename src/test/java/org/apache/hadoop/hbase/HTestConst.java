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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Collections;

import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

/** Similar to {@link HConstants} but for tests. */
public class HTestConst {

  private HTestConst() {
  }

  public static final String DEFAULT_TABLE_STR = "MyTestTable";
  public static final byte[] DEFAULT_TABLE_BYTES = Bytes.toBytes(DEFAULT_TABLE_STR);
  public static final ByteBuffer DEFAULT_TABLE_BYTE_BUF = ByteBuffer.wrap(DEFAULT_TABLE_BYTES);

  public static final String DEFAULT_CF_STR = "MyDefaultCF";
  public static final byte[] DEFAULT_CF_BYTES = Bytes.toBytes(DEFAULT_CF_STR);

  public static final Set<String> DEFAULT_CF_STR_SET =
      Collections.unmodifiableSet(new HashSet<String>(
          Arrays.asList(new String[] { DEFAULT_CF_STR })));

  public static final List<ColumnDescriptor> DEFAULT_COLUMN_DESC_LIST;

  public static byte[] getRowBytes(int i) {
    return Bytes.toBytes("row" + i);
  }

  public static String getQualStr(int i) {
    return "column" + i;
  }

  public static byte[] getCFQualBytes(int i) {
    return Bytes.toBytes(DEFAULT_CF_STR + KeyValue.COLUMN_FAMILY_DELIMITER + getQualStr(i));
  }

  static {
    List<ColumnDescriptor> cdList = new ArrayList<ColumnDescriptor>();
    ColumnDescriptor cd = new ColumnDescriptor();
    cd.setName(DEFAULT_CF_BYTES);
    cdList.add(cd);
    DEFAULT_COLUMN_DESC_LIST = Collections.unmodifiableList(cdList);
  }

}
