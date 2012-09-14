/**
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
package org.apache.hadoop.hbase.filter;

import java.util.Set;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFirstKeyValueMatchingQualifiersFilter extends TestCase {
  private static final byte[] ROW = Bytes.toBytes("test");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test");
  private static final byte[] COLUMN_QUALIFIER_1 = Bytes.toBytes("foo");
  private static final byte[] COLUMN_QUALIFIER_2 = Bytes.toBytes("foo_2");
  private static final byte[] COLUMN_QUALIFIER_3 = Bytes.toBytes("foo_3");
  private static final byte[] VAL_1 = Bytes.toBytes("a");

  /**
   * Test the functionality of
   * {@link FirstKeyValueMatchingQualifiersFilter#filterKeyValue(KeyValue)}
   * 
   * @throws Exception
   */
  public void testFirstKeyMatchingQualifierFilter() throws Exception {
    Set<byte[]> quals = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    quals.add(COLUMN_QUALIFIER_1);
    quals.add(COLUMN_QUALIFIER_2);
    Filter filter = new FirstKeyValueMatchingQualifiersFilter(quals);

    // Match in first attempt
    KeyValue kv;
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_1, VAL_1);
    assertTrue("includeAndSetFlag",
        filter.filterKeyValue(kv) == Filter.ReturnCode.INCLUDE);
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    assertTrue("flagIsSetSkipToNextRow",
        filter.filterKeyValue(kv) == Filter.ReturnCode.NEXT_ROW);

    // A mismatch in first attempt and match in second attempt.
    filter.reset();
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_3, VAL_1);
    System.out.println(filter.filterKeyValue(kv));
    assertTrue("includeFlagIsUnset",
        filter.filterKeyValue(kv) == Filter.ReturnCode.INCLUDE);
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_2, VAL_1);
    assertTrue("includeAndSetFlag",
        filter.filterKeyValue(kv) == Filter.ReturnCode.INCLUDE);
    kv = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER_1, VAL_1);
    assertTrue("flagIsSetSkipToNextRow",
        filter.filterKeyValue(kv) == Filter.ReturnCode.NEXT_ROW);
  }

}
