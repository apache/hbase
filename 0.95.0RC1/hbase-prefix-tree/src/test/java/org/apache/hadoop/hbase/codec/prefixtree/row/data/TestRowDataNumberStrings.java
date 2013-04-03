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

package org.apache.hadoop.hbase.codec.prefixtree.row.data;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class TestRowDataNumberStrings extends BaseTestRowData{

  static List<KeyValue> d = Lists.newArrayList();
  static {

  /**
   * Test a string-encoded list of numbers.  0, 1, 10, 11 will sort as 0, 1, 10, 11 if strings
   * <p/>
   * This helped catch a bug with reverse scanning where it was jumping from the last leaf cell to
   * the previous nub.  It should do 11->10, but it was incorrectly doing 11->1
   */
    List<Integer> problematicSeries = Lists.newArrayList(0, 1, 10, 11);//sort this at the end
    for(Integer i : problematicSeries){
//    for(int i=0; i < 13; ++i){
      byte[] row = Bytes.toBytes(""+i);
      byte[] family = Bytes.toBytes("F");
      byte[] column = Bytes.toBytes("C");
      byte[] value = Bytes.toBytes("V");

      d.add(new KeyValue(row, family, column, 0L, Type.Put, value));
    }
    Collections.sort(d, new CellComparator());
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

}
