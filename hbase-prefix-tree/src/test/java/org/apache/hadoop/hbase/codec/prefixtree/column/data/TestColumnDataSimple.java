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

package org.apache.hadoop.hbase.codec.prefixtree.column.data;

import java.util.List;

import org.apache.hadoop.hbase.codec.prefixtree.column.TestColumnData;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.ByteRangeUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class TestColumnDataSimple implements TestColumnData {

  @Override
  public List<ByteRange> getInputs() {
    List<String> d = Lists.newArrayList();
    d.add("abc");
    d.add("abcde");
    d.add("abc");
    d.add("bbc");
    d.add("abc");
    return ByteRangeUtils.fromArrays(Bytes.getUtf8ByteArrays(d));
  }

  @Override
  public List<ByteRange> getOutputs() {
    List<String> d = Lists.newArrayList();
    d.add("abc");
    d.add("abcde");
    d.add("bbc");
    return ByteRangeUtils.fromArrays(Bytes.getUtf8ByteArrays(d));
  }

}
