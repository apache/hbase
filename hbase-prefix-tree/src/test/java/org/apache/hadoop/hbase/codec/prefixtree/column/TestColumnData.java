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

package org.apache.hadoop.hbase.codec.prefixtree.column;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.codec.prefixtree.column.data.TestColumnDataRandom;
import org.apache.hadoop.hbase.codec.prefixtree.column.data.TestColumnDataSimple;
import org.apache.hadoop.hbase.util.ByteRange;

import com.google.common.collect.Lists;

public interface TestColumnData {

  List<ByteRange> getInputs();
  List<ByteRange> getOutputs();

  class InMemory {
    public Collection<Object[]> getAllAsObjectArray() {
      List<Object[]> all = Lists.newArrayList();
      all.add(new Object[] { new TestColumnDataSimple() });
      for (int leftShift = 0; leftShift < 16; ++leftShift) {
        all.add(new Object[] { new TestColumnDataRandom(1 << leftShift) });
      }
      return all;
    }
  }
}
