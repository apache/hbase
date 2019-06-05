package org.apache.hadoop.hbase.hbtop;

import static org.apache.hadoop.hbase.hbtop.Record.entry;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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

    assertThat(actual.get(Field.TABLE).asString(), is("tableName"));
    assertThat(actual.get(Field.REGION_COUNT).asInt(), is(8));
    assertThat(actual.get(Field.REQUEST_COUNT_PER_SECOND).asLong(), is(600L));
  }

  @Test
  public void testToImmutable() {
    Record record = Record.ofEntries(
      entry(Field.TABLE, "tableName"),
      entry(Field.REGION_COUNT, 3),
      entry(Field.REQUEST_COUNT_PER_SECOND, 100L)
    );

    record = record.toImmutable();

    assertThat(record.get(Field.TABLE).asString(), is("tableName"));

    try {
      record.put(Field.TABLE, "tableName2");
      fail();
    } catch(UnsupportedOperationException ignored) {
    }
  }
}
