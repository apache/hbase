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

package org.apache.hadoop.hbase.codec.prefixtree.row;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataComplexQualifiers;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataDeeper;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataDifferentTimestamps;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataEmpty;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataExerciseFInts;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataNub;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataNumberStrings;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataQualifierByteOrdering;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataRandomKeyValues;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataRandomKeyValuesWithTags;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataSearchWithPrefix;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataSearcherRowMiss;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataSimple;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataSingleQualifier;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataTrivial;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataTrivialWithTags;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataUrls;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataUrlsExample;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher;

import com.google.common.collect.Lists;

/*
 * A master class for registering different implementations of TestRowData.
 */
public interface TestRowData {

  List<KeyValue> getInputs();
  List<Integer> getRowStartIndexes();

  void individualBlockMetaAssertions(PrefixTreeBlockMeta blockMeta);

  void individualSearcherAssertions(CellSearcher searcher);

  static class InMemory {

    /*
     * The following are different styles of data that the codec may encounter.  Having these small
     * representations of the data helps pinpoint what is wrong if the encoder breaks.
     */
    public static Collection<TestRowData> getAll() {
      List<TestRowData> all = Lists.newArrayList();
      //simple
      all.add(new TestRowDataEmpty());
      all.add(new TestRowDataTrivial());
      all.add(new TestRowDataTrivialWithTags());
      all.add(new TestRowDataSimple());
      all.add(new TestRowDataDeeper());

      //more specific
      all.add(new TestRowDataSingleQualifier());
//      all.add(new TestRowDataMultiFamilies());//multiple families disabled in PrefixTreeEncoder
      all.add(new TestRowDataNub());
      all.add(new TestRowDataSearcherRowMiss());
      all.add(new TestRowDataQualifierByteOrdering());
      all.add(new TestRowDataComplexQualifiers());
      all.add(new TestRowDataDifferentTimestamps());

      //larger data volumes (hard to debug)
      all.add(new TestRowDataNumberStrings());
      all.add(new TestRowDataUrls());
      all.add(new TestRowDataUrlsExample());
      all.add(new TestRowDataExerciseFInts());
      all.add(new TestRowDataRandomKeyValues());
      all.add(new TestRowDataRandomKeyValuesWithTags());
      
      //test data for HBase-12078
      all.add(new TestRowDataSearchWithPrefix());
      return all;
    }

    public static Collection<Object[]> getAllAsObjectArray() {
      List<Object[]> all = Lists.newArrayList();
      for (TestRowData testRows : getAll()) {
        all.add(new Object[] { testRows });
      }
      return all;
    }
  }
}
