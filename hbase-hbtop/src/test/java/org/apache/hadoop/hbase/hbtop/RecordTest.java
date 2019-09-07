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
package org.apache.hadoop.hbase.hbtop;

import static org.apache.hadoop.hbase.hbtop.Record.entry;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class RecordTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(RecordTest.class);

  @Test
  public void testBuilder() {
    Record actual1 = Record.builder().put(Field.TABLE, "tableName")
      .put(entry(Field.REGION_COUNT, 3))
      .put(Field.REQUEST_COUNT_PER_SECOND, Field.REQUEST_COUNT_PER_SECOND.newValue(100L))
      .build();

    assertThat(actual1.size(), is(3));
    assertThat(actual1.get(Field.TABLE).asString(), is("tableName"));
    assertThat(actual1.get(Field.REGION_COUNT).asInt(), is(3));
    assertThat(actual1.get(Field.REQUEST_COUNT_PER_SECOND).asLong(), is(100L));

    Record actual2 = Record.builder().putAll(actual1).build();

    assertThat(actual2.size(), is(3));
    assertThat(actual2.get(Field.TABLE).asString(), is("tableName"));
    assertThat(actual2.get(Field.REGION_COUNT).asInt(), is(3));
    assertThat(actual2.get(Field.REQUEST_COUNT_PER_SECOND).asLong(), is(100L));
  }

  @Test
  public void testOfEntries() {
    Record actual = Record.ofEntries(
      entry(Field.TABLE, "tableName"),
      entry(Field.REGION_COUNT, 3),
      entry(Field.REQUEST_COUNT_PER_SECOND, 100L)
    );

    assertThat(actual.size(), is(3));
    assertThat(actual.get(Field.TABLE).asString(), is("tableName"));
    assertThat(actual.get(Field.REGION_COUNT).asInt(), is(3));
    assertThat(actual.get(Field.REQUEST_COUNT_PER_SECOND).asLong(), is(100L));
  }

  @Test
  public void testCombine() {
    Record record1 = Record.ofEntries(
      entry(Field.TABLE, "tableName"),
      entry(Field.REGION_COUNT, 3),
      entry(Field.REQUEST_COUNT_PER_SECOND, 100L)
    );

    Record record2 = Record.ofEntries(
      entry(Field.TABLE, "tableName"),
      entry(Field.REGION_COUNT, 5),
      entry(Field.REQUEST_COUNT_PER_SECOND, 500L)
    );

    Record actual = record1.combine(record2);

    assertThat(actual.size(), is(3));
    assertThat(actual.get(Field.TABLE).asString(), is("tableName"));
    assertThat(actual.get(Field.REGION_COUNT).asInt(), is(8));
    assertThat(actual.get(Field.REQUEST_COUNT_PER_SECOND).asLong(), is(600L));
  }
}
