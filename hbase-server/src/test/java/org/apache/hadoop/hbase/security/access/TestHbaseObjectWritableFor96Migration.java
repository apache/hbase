/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.Message;

@Category({SmallTests.class})
public class TestHbaseObjectWritableFor96Migration {

  @Test
  public void testCustomWritable() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // test proper serialization of un-encoded custom writables
    CustomWritable custom = new CustomWritable("test phrase");
    Object obj = doType(conf, custom, CustomWritable.class);
    assertTrue(obj instanceof Writable);
    assertTrue(obj instanceof CustomWritable);
    assertEquals("test phrase", ((CustomWritable)obj).getValue());
  }

  @Test
  public void testCustomSerializable() throws Exception {
    Configuration conf = HBaseConfiguration.create();
  
    Configuration legacyConf = HBaseConfiguration.create();
    legacyConf.setBoolean(HConstants.ALLOW_LEGACY_OBJECT_SERIALIZATION_KEY, true);

    CustomSerializable custom = new CustomSerializable("test phrase");

    // check that we can't write by default
    try {
      writeType(conf, custom, CustomSerializable.class);
      fail("IOException expected");
    } catch (IOException e) {
      // expected
    }

    // check that we can't read by default
    byte[] data = writeType(legacyConf, custom, CustomSerializable.class);
    try {
      readType(conf, data);
      fail("IOException expected");
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testLegacyCustomSerializable() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.ALLOW_LEGACY_OBJECT_SERIALIZATION_KEY, true);
    // test proper serialization of un-encoded serialized java objects
    CustomSerializable custom = new CustomSerializable("test phrase");
    byte[] data = writeType(conf, custom, CustomSerializable.class);
    Object obj = readType(conf, data);
    assertTrue(obj instanceof Serializable);
    assertTrue(obj instanceof CustomSerializable);
    assertEquals("test phrase", ((CustomSerializable)obj).getValue());
  }

  private Object doType(final Configuration conf, final Object value,
      final Class<?> clazz)
  throws IOException {
    return readType(conf, writeType(conf, value, clazz));
  }

  @SuppressWarnings("deprecation")
  private byte[] writeType(final Configuration conf, final Object value,
      final Class<?> clazz) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    HbaseObjectWritableFor96Migration.writeObject(out, value, clazz, conf);
    out.close();
    return byteStream.toByteArray();
  }

  @SuppressWarnings("deprecation")
  private Object readType(final Configuration conf, final byte[] value)
      throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(value));
    Object product = HbaseObjectWritableFor96Migration.readObject(dis, conf);
    dis.close();
    return product;
  }

  public static class CustomSerializable implements Serializable {
    private static final long serialVersionUID = 1048445561865740632L;
    private String value = null;

    public CustomSerializable() {
    }

    public CustomSerializable(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

  }

  public static class CustomWritable implements Writable {
    private String value = null;

    public CustomWritable() {
    }

    public CustomWritable(String val) {
      this.value = val;
    }

    public String getValue() { return value; }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, this.value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.value = Text.readString(in);
    }
  }

  /**
   * Test case to ensure ordering of CODE_TO_CLASS and CLASS_TO_CODE. In the
   * past, and item was added in the middle of the static initializer, and that
   * threw off all of the codes after the addition. This unintentionally broke
   * the wire protocol for clients. The idea behind this test case is that if
   * you unintentionally change the order, you will get a test failure. If you
   * are actually intentionally change the order, just update the test case.
   * This should be a clue to the reviewer that you are doing something to
   * change the wire protocol.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testGetClassCode() throws IOException{
    // Primitive types
    assertEquals(1,HbaseObjectWritableFor96Migration.getClassCode(Boolean.TYPE).intValue());
    assertEquals(2,HbaseObjectWritableFor96Migration.getClassCode(Byte.TYPE).intValue());
    assertEquals(3,HbaseObjectWritableFor96Migration.getClassCode(Character.TYPE).intValue());
    assertEquals(4,HbaseObjectWritableFor96Migration.getClassCode(Short.TYPE).intValue());
    assertEquals(5,HbaseObjectWritableFor96Migration.getClassCode(Integer.TYPE).intValue());
    assertEquals(6,HbaseObjectWritableFor96Migration.getClassCode(Long.TYPE).intValue());
    assertEquals(7,HbaseObjectWritableFor96Migration.getClassCode(Float.TYPE).intValue());
    assertEquals(8,HbaseObjectWritableFor96Migration.getClassCode(Double.TYPE).intValue());
    assertEquals(9,HbaseObjectWritableFor96Migration.getClassCode(Void.TYPE).intValue());

    // Other java types
    assertEquals(10,HbaseObjectWritableFor96Migration.getClassCode(String.class).intValue());
    assertEquals(11,HbaseObjectWritableFor96Migration.getClassCode(byte [].class).intValue());
    assertEquals(12,HbaseObjectWritableFor96Migration.getClassCode(byte [][].class).intValue());

    // Hadoop types
    assertEquals(13,HbaseObjectWritableFor96Migration.getClassCode(Text.class).intValue());
    assertEquals(14,HbaseObjectWritableFor96Migration.getClassCode(Writable.class).intValue());
    assertEquals(15,HbaseObjectWritableFor96Migration.getClassCode(Writable [].class).intValue());
    //assertEquals(16,HbaseObjectWritableFor96Migration.getClassCode(HbaseMapWritable.class).intValue());
    // 17 is NullInstance which isn't visible from here

    // Hbase types
    assertEquals(18,HbaseObjectWritableFor96Migration.getClassCode(HColumnDescriptor.class).intValue());
    assertEquals(19,HbaseObjectWritableFor96Migration.getClassCode(HConstants.Modify.class).intValue());
    // 20 and 21 are place holders for HMsg
    // 22: Not pushed over the wire
    // 23: Not pushed over the wire
    assertEquals(24,HbaseObjectWritableFor96Migration.getClassCode(HRegionInfo.class).intValue());
    assertEquals(25,HbaseObjectWritableFor96Migration.getClassCode(HRegionInfo[].class).intValue());
    // 26: Removed
    // 27: Removed
    assertEquals(28,HbaseObjectWritableFor96Migration.getClassCode(HTableDescriptor.class).intValue());
    assertEquals(29,HbaseObjectWritableFor96Migration.getClassCode(MapWritable.class).intValue());

    // HBASE-880
    assertEquals(30,HbaseObjectWritableFor96Migration.getClassCode(ClusterStatus.class).intValue());
    assertEquals(31,HbaseObjectWritableFor96Migration.getClassCode(Delete.class).intValue());
    assertEquals(32,HbaseObjectWritableFor96Migration.getClassCode(Get.class).intValue());
    assertEquals(33,HbaseObjectWritableFor96Migration.getClassCode(KeyValue.class).intValue());
    assertEquals(34,HbaseObjectWritableFor96Migration.getClassCode(KeyValue[].class).intValue());
    assertEquals(35,HbaseObjectWritableFor96Migration.getClassCode(Put.class).intValue());
    assertEquals(36,HbaseObjectWritableFor96Migration.getClassCode(Put[].class).intValue());
    assertEquals(37,HbaseObjectWritableFor96Migration.getClassCode(Result.class).intValue());
    assertEquals(38,HbaseObjectWritableFor96Migration.getClassCode(Result[].class).intValue());
    assertEquals(39,HbaseObjectWritableFor96Migration.getClassCode(Scan.class).intValue());

    assertEquals(40,HbaseObjectWritableFor96Migration.getClassCode(WhileMatchFilter.class).intValue());
    assertEquals(41,HbaseObjectWritableFor96Migration.getClassCode(PrefixFilter.class).intValue());
    assertEquals(42,HbaseObjectWritableFor96Migration.getClassCode(PageFilter.class).intValue());
    assertEquals(43,HbaseObjectWritableFor96Migration.getClassCode(InclusiveStopFilter.class).intValue());
    assertEquals(44,HbaseObjectWritableFor96Migration.getClassCode(ColumnCountGetFilter.class).intValue());
    assertEquals(45,HbaseObjectWritableFor96Migration.getClassCode(SingleColumnValueFilter.class).intValue());
    assertEquals(46,HbaseObjectWritableFor96Migration.getClassCode(SingleColumnValueExcludeFilter.class).intValue());
    assertEquals(47,HbaseObjectWritableFor96Migration.getClassCode(BinaryComparator.class).intValue());
    assertEquals(48,HbaseObjectWritableFor96Migration.getClassCode(BitComparator.class).intValue());
    assertEquals(49,HbaseObjectWritableFor96Migration.getClassCode(CompareFilter.class).intValue());
    assertEquals(50,HbaseObjectWritableFor96Migration.getClassCode(RowFilter.class).intValue());
    assertEquals(51,HbaseObjectWritableFor96Migration.getClassCode(ValueFilter.class).intValue());
    assertEquals(52,HbaseObjectWritableFor96Migration.getClassCode(QualifierFilter.class).intValue());
    assertEquals(53,HbaseObjectWritableFor96Migration.getClassCode(SkipFilter.class).intValue());
    // assertEquals(54,HbaseObjectWritableFor96Migration.getClassCode(WritableByteArrayComparable.class).intValue());
    assertEquals(55,HbaseObjectWritableFor96Migration.getClassCode(FirstKeyOnlyFilter.class).intValue());
    assertEquals(56,HbaseObjectWritableFor96Migration.getClassCode(DependentColumnFilter.class).intValue());

    assertEquals(57,HbaseObjectWritableFor96Migration.getClassCode(Delete [].class).intValue());

    assertEquals(58,HbaseObjectWritableFor96Migration.getClassCode(HLog.Entry.class).intValue());
    assertEquals(59,HbaseObjectWritableFor96Migration.getClassCode(HLog.Entry[].class).intValue());
    assertEquals(60,HbaseObjectWritableFor96Migration.getClassCode(HLogKey.class).intValue());

    assertEquals(61,HbaseObjectWritableFor96Migration.getClassCode(List.class).intValue());

    assertEquals(62,HbaseObjectWritableFor96Migration.getClassCode(NavigableSet.class).intValue());
    assertEquals(63,HbaseObjectWritableFor96Migration.getClassCode(ColumnPrefixFilter.class).intValue());

    // Multi
    assertEquals(64,HbaseObjectWritableFor96Migration.getClassCode(Row.class).intValue());
    assertEquals(65,HbaseObjectWritableFor96Migration.getClassCode(Action.class).intValue());
    assertEquals(66,HbaseObjectWritableFor96Migration.getClassCode(MultiAction.class).intValue());
    assertEquals(67,HbaseObjectWritableFor96Migration.getClassCode(MultiResponse.class).intValue());

    // coprocessor execution
    // assertEquals(68,HbaseObjectWritableFor96Migration.getClassCode(Exec.class).intValue());
    assertEquals(69,HbaseObjectWritableFor96Migration.getClassCode(Increment.class).intValue());

    assertEquals(70,HbaseObjectWritableFor96Migration.getClassCode(KeyOnlyFilter.class).intValue());

    // serializable
    assertEquals(71,HbaseObjectWritableFor96Migration.getClassCode(Serializable.class).intValue());
    assertEquals(72,HbaseObjectWritableFor96Migration.getClassCode(RandomRowFilter.class).intValue());
    assertEquals(73,HbaseObjectWritableFor96Migration.getClassCode(CompareOp.class).intValue());
    assertEquals(74,HbaseObjectWritableFor96Migration.getClassCode(ColumnRangeFilter.class).intValue());
    // assertEquals(75,HbaseObjectWritableFor96Migration.getClassCode(HServerLoad.class).intValue());
    assertEquals(76,HbaseObjectWritableFor96Migration.getClassCode(RegionOpeningState.class).intValue());
    assertEquals(77,HbaseObjectWritableFor96Migration.getClassCode(HTableDescriptor[].class).intValue());
    assertEquals(78,HbaseObjectWritableFor96Migration.getClassCode(Append.class).intValue());
    assertEquals(79,HbaseObjectWritableFor96Migration.getClassCode(RowMutations.class).intValue());
    assertEquals(80,HbaseObjectWritableFor96Migration.getClassCode(Message.class).intValue());

    assertEquals(81,HbaseObjectWritableFor96Migration.getClassCode(Array.class).intValue());
  }

  /**
   * This test verifies that additional objects have not been added to the end of the list.
   * If you are legitimately adding objects, this test will need to be updated, but see the
   * note on the test above. 
   */
  public void testGetNextObjectCode(){
    assertEquals(83,HbaseObjectWritableFor96Migration.getNextClassCode());
  }

}
