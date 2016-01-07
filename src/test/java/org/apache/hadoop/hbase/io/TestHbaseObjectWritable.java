/**
 *
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
package org.apache.hadoop.hbase.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
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
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
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
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;

@Category(SmallTests.class)
public class TestHbaseObjectWritable extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  @SuppressWarnings("boxing")
  public void testReadOldObjectDataInput() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    /*
     * This is the code used to generate byte[] where
     *  HbaseObjectWritable used byte for code
     *
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    HbaseObjectWritable.writeObject(out, bytes, byte[].class, conf);
    byte[] ba = byteStream.toByteArray();
    out.close();
    */

    /*
     * byte array generated by the folowing call
     *  HbaseObjectWritable.writeObject(out, new Text("Old"), Text.class, conf);
     */
    byte[] baForText = {13, 13, 3, 79, 108, 100};
    Text txt = (Text)readByteArray(conf, baForText);
    Text oldTxt = new Text("Old");
    assertEquals(txt, oldTxt);

    final byte A = 'A';
    byte [] bytes = new byte[1];
    bytes[0] = A;
    /*
     * byte array generated by the folowing call
     *  HbaseObjectWritable.writeObject(out, bytes, byte[].class, conf);
     */
    byte[] baForByteArray = { 11, 1, 65 };
    byte[] baOut = (byte[])readByteArray(conf, baForByteArray);
    assertTrue(Bytes.equals(baOut, bytes));
  }

  /*
   * helper method which reads byte array using HbaseObjectWritable.readObject()
   */
  private Object readByteArray(final Configuration conf, final byte[] ba)
  throws IOException {
    ByteArrayInputStream bais =
      new ByteArrayInputStream(ba);
    DataInputStream dis = new DataInputStream(bais);
    Object product = HbaseObjectWritable.readObject(dis, conf);
    dis.close();
    return product;
  }

  @SuppressWarnings("boxing")
  public void testReadObjectDataInputConfiguration() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // Do primitive type
    final int COUNT = 101;
    assertTrue(doType(conf, COUNT, int.class).equals(COUNT));
    // Do array
    final byte [] testing = "testing".getBytes();
    byte [] result = (byte [])doType(conf, testing, testing.getClass());
    assertTrue(WritableComparator.compareBytes(testing, 0, testing.length,
       result, 0, result.length) == 0);
    // Do unsupported type.
    boolean exception = false;
    try {
      doType(conf, new Object(), Object.class);
    } catch (UnsupportedOperationException uoe) {
      exception = true;
    }
    assertTrue(exception);
    // Try odd types
    final byte A = 'A';
    byte [] bytes = new byte[1];
    bytes[0] = A;
    Object obj = doType(conf, bytes, byte [].class);
    assertTrue(((byte [])obj)[0] == A);
    // Do 'known' Writable type.
    obj = doType(conf, new Text(""), Text.class);
    assertTrue(obj instanceof Text);
    //List.class
    List<String> list = new ArrayList<String>();
    list.add("hello");
    list.add("world");
    list.add("universe");
    obj = doType(conf, list, List.class);
    assertTrue(obj instanceof List);
    Assert.assertArrayEquals(list.toArray(), ((List)obj).toArray() );
    //List.class with null values
    List<String> listWithNulls = new ArrayList<String>();
    listWithNulls.add("hello");
    listWithNulls.add("world");
    listWithNulls.add(null);
    obj = doType(conf, listWithNulls, List.class);
    assertTrue(obj instanceof List);
    Assert.assertArrayEquals(listWithNulls.toArray(), ((List)obj).toArray() );
    //ArrayList.class
    ArrayList<String> arr = new ArrayList<String>();
    arr.add("hello");
    arr.add("world");
    arr.add("universe");
    obj = doType(conf,  arr, ArrayList.class);
    assertTrue(obj instanceof ArrayList);
    Assert.assertArrayEquals(list.toArray(), ((ArrayList)obj).toArray() );
    // Check that filters can be serialized
    obj = doType(conf, new PrefixFilter(HConstants.EMPTY_BYTE_ARRAY),
      PrefixFilter.class);
    assertTrue(obj instanceof PrefixFilter);
  }

  public void testCustomWritable() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    // test proper serialization of un-encoded custom writables
    CustomWritable custom = new CustomWritable("test phrase");
    Object obj = doType(conf, custom, CustomWritable.class);
    assertTrue(obj instanceof Writable);
    assertTrue(obj instanceof CustomWritable);
    assertEquals("test phrase", ((CustomWritable)obj).getValue());

    // test proper serialization of a custom filter
    CustomFilter filt = new CustomFilter("mykey");
    FilterList filtlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filtlist.addFilter(filt);
    obj = doType(conf, filtlist, FilterList.class);
    assertTrue(obj instanceof FilterList);
    assertNotNull(((FilterList)obj).getFilters());
    assertEquals(1, ((FilterList)obj).getFilters().size());
    Filter child = ((FilterList)obj).getFilters().get(0);
    assertTrue(child instanceof CustomFilter);
    assertEquals("mykey", ((CustomFilter)child).getKey());
  }

  public void testCustomSerializable() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    // test proper serialization of un-encoded serialized java objects
    CustomSerializable custom = new CustomSerializable("test phrase");
    Object obj = doType(conf, custom, CustomSerializable.class);
    assertTrue(obj instanceof Serializable);
    assertTrue(obj instanceof CustomSerializable);
    assertEquals("test phrase", ((CustomSerializable)obj).getValue());
  }

  private Object doType(final Configuration conf, final Object value,
      final Class<?> clazz)
  throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    HbaseObjectWritable.writeObject(out, value, clazz, conf);
    out.close();
    ByteArrayInputStream bais =
      new ByteArrayInputStream(byteStream.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    Object product = HbaseObjectWritable.readObject(dis, conf);
    dis.close();
    return product;
  }

  public static class A extends IntWritable {
    public A() {}
    public A(int a) {super(a);}
  }

  public static class B extends A {
    int b;
    public B() { }
    public B(int a, int b) {
      super(a);
      this.b = b;
    }
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(b);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      this.b = in.readInt();
    }
    @Override
    public boolean equals(Object o) {
      if (o instanceof B) {
        return this.get() == ((B) o).get() && this.b == ((B) o).b;
      }
      return false;
    }
  }

  /** Tests for serialization of List and Arrays */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testPolymorphismInSequences() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Object ret;

    //test with lists
    List<A> list = Lists.newArrayList(new A(42), new B(10, 100));
    ret = doType(conf, list, list.getClass());
    assertEquals(ret, list);

    //test with Writable[]
    Writable[] warr = new Writable[] {new A(42), new B(10, 100)};
    ret = doType(conf, warr, warr.getClass());
    Assert.assertArrayEquals((Writable[])ret, warr);

    //test with arrays
    A[] arr = new A[] {new A(42), new B(10, 100)};
    ret = doType(conf, arr, arr.getClass());
    Assert.assertArrayEquals((A[])ret, arr);

    //test with double array
    A[][] darr = new A[][] {new A[] { new A(42), new B(10, 100)}, new A[] {new A(12)}};
    ret = doType(conf, darr, darr.getClass());
    Assert.assertArrayEquals((A[][])ret, darr);

    //test with List of arrays
    List<A[]> larr = Lists.newArrayList(arr, new A[] {new A(99)});
    ret = doType(conf, larr, larr.getClass());
    List<A[]> lret = (List<A[]>) ret;
    assertEquals(larr.size(), lret.size());
    for (int i=0; i<lret.size(); i++) {
      Assert.assertArrayEquals(larr.get(i), lret.get(i));
    }

    //test with array of lists
    List[] alarr = new List[] {Lists.newArrayList(new A(1), new A(2)),
        Lists.newArrayList(new B(4,5))};
    ret = doType(conf, alarr, alarr.getClass());
    List[] alret = (List[]) ret;
    Assert.assertArrayEquals(alarr, alret);

    //test with array of Text, note that Text[] is not pre-defined
    Text[] tarr = new Text[] {new Text("foo"), new Text("bar")};
    ret = doType(conf, tarr, tarr.getClass());
    Assert.assertArrayEquals(tarr, (Text[])ret);

    //test with byte[][]
    byte[][] barr = new byte[][] {"foo".getBytes(), "baz".getBytes()};
    ret = doType(conf, barr, barr.getClass());
    Assert.assertArrayEquals(barr, (byte[][])ret);
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

  public static class CustomFilter extends FilterBase {
    private String key = null;

    public CustomFilter() {
    }

    public CustomFilter(String key) {
      this.key = key;
    }

    public String getKey() { return key; }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, this.key);
    }

    public void readFields(DataInput in) throws IOException {
      this.key = Text.readString(in);
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
  public void testGetClassCode() throws IOException{
    // Primitive types
    assertEquals(1,HbaseObjectWritable.getClassCode(Boolean.TYPE).intValue());
    assertEquals(2,HbaseObjectWritable.getClassCode(Byte.TYPE).intValue());
    assertEquals(3,HbaseObjectWritable.getClassCode(Character.TYPE).intValue());
    assertEquals(4,HbaseObjectWritable.getClassCode(Short.TYPE).intValue());
    assertEquals(5,HbaseObjectWritable.getClassCode(Integer.TYPE).intValue());
    assertEquals(6,HbaseObjectWritable.getClassCode(Long.TYPE).intValue());
    assertEquals(7,HbaseObjectWritable.getClassCode(Float.TYPE).intValue());
    assertEquals(8,HbaseObjectWritable.getClassCode(Double.TYPE).intValue());
    assertEquals(9,HbaseObjectWritable.getClassCode(Void.TYPE).intValue());

    // Other java types
    assertEquals(10,HbaseObjectWritable.getClassCode(String.class).intValue());
    assertEquals(11,HbaseObjectWritable.getClassCode(byte [].class).intValue());
    assertEquals(12,HbaseObjectWritable.getClassCode(byte [][].class).intValue());

    // Hadoop types
    assertEquals(13,HbaseObjectWritable.getClassCode(Text.class).intValue());
    assertEquals(14,HbaseObjectWritable.getClassCode(Writable.class).intValue());
    assertEquals(15,HbaseObjectWritable.getClassCode(Writable [].class).intValue());
    assertEquals(16,HbaseObjectWritable.getClassCode(HbaseMapWritable.class).intValue());
    // 17 is NullInstance which isn't visible from here

    // Hbase types
    assertEquals(18,HbaseObjectWritable.getClassCode(HColumnDescriptor.class).intValue());
    assertEquals(19,HbaseObjectWritable.getClassCode(HConstants.Modify.class).intValue());
    // 20 and 21 are place holders for HMsg
    assertEquals(22,HbaseObjectWritable.getClassCode(HRegion.class).intValue());
    assertEquals(23,HbaseObjectWritable.getClassCode(HRegion[].class).intValue());
    assertEquals(24,HbaseObjectWritable.getClassCode(HRegionInfo.class).intValue());
    assertEquals(25,HbaseObjectWritable.getClassCode(HRegionInfo[].class).intValue());
    assertEquals(26,HbaseObjectWritable.getClassCode(HServerAddress.class).intValue());
    assertEquals(27,HbaseObjectWritable.getClassCode(HServerInfo.class).intValue());
    assertEquals(28,HbaseObjectWritable.getClassCode(HTableDescriptor.class).intValue());
    assertEquals(29,HbaseObjectWritable.getClassCode(MapWritable.class).intValue());

    // HBASE-880
    assertEquals(30,HbaseObjectWritable.getClassCode(ClusterStatus.class).intValue());
    assertEquals(31,HbaseObjectWritable.getClassCode(Delete.class).intValue());
    assertEquals(32,HbaseObjectWritable.getClassCode(Get.class).intValue());
    assertEquals(33,HbaseObjectWritable.getClassCode(KeyValue.class).intValue());
    assertEquals(34,HbaseObjectWritable.getClassCode(KeyValue[].class).intValue());
    assertEquals(35,HbaseObjectWritable.getClassCode(Put.class).intValue());
    assertEquals(36,HbaseObjectWritable.getClassCode(Put[].class).intValue());
    assertEquals(37,HbaseObjectWritable.getClassCode(Result.class).intValue());
    assertEquals(38,HbaseObjectWritable.getClassCode(Result[].class).intValue());
    assertEquals(39,HbaseObjectWritable.getClassCode(Scan.class).intValue());

    assertEquals(40,HbaseObjectWritable.getClassCode(WhileMatchFilter.class).intValue());
    assertEquals(41,HbaseObjectWritable.getClassCode(PrefixFilter.class).intValue());
    assertEquals(42,HbaseObjectWritable.getClassCode(PageFilter.class).intValue());
    assertEquals(43,HbaseObjectWritable.getClassCode(InclusiveStopFilter.class).intValue());
    assertEquals(44,HbaseObjectWritable.getClassCode(ColumnCountGetFilter.class).intValue());
    assertEquals(45,HbaseObjectWritable.getClassCode(SingleColumnValueFilter.class).intValue());
    assertEquals(46,HbaseObjectWritable.getClassCode(SingleColumnValueExcludeFilter.class).intValue());
    assertEquals(47,HbaseObjectWritable.getClassCode(BinaryComparator.class).intValue());
    assertEquals(48,HbaseObjectWritable.getClassCode(BitComparator.class).intValue());
    assertEquals(49,HbaseObjectWritable.getClassCode(CompareFilter.class).intValue());
    assertEquals(50,HbaseObjectWritable.getClassCode(RowFilter.class).intValue());
    assertEquals(51,HbaseObjectWritable.getClassCode(ValueFilter.class).intValue());
    assertEquals(52,HbaseObjectWritable.getClassCode(QualifierFilter.class).intValue());
    assertEquals(53,HbaseObjectWritable.getClassCode(SkipFilter.class).intValue());
    assertEquals(54,HbaseObjectWritable.getClassCode(WritableByteArrayComparable.class).intValue());
    assertEquals(55,HbaseObjectWritable.getClassCode(FirstKeyOnlyFilter.class).intValue());
    assertEquals(56,HbaseObjectWritable.getClassCode(DependentColumnFilter.class).intValue());

    assertEquals(57,HbaseObjectWritable.getClassCode(Delete [].class).intValue());

    assertEquals(58,HbaseObjectWritable.getClassCode(HLog.Entry.class).intValue());
    assertEquals(59,HbaseObjectWritable.getClassCode(HLog.Entry[].class).intValue());
    assertEquals(60,HbaseObjectWritable.getClassCode(HLogKey.class).intValue());

    assertEquals(61,HbaseObjectWritable.getClassCode(List.class).intValue());

    assertEquals(62,HbaseObjectWritable.getClassCode(NavigableSet.class).intValue());
    assertEquals(63,HbaseObjectWritable.getClassCode(ColumnPrefixFilter.class).intValue());

    // Multi
    assertEquals(64,HbaseObjectWritable.getClassCode(Row.class).intValue());
    assertEquals(65,HbaseObjectWritable.getClassCode(Action.class).intValue());
    assertEquals(66,HbaseObjectWritable.getClassCode(MultiAction.class).intValue());
    assertEquals(67,HbaseObjectWritable.getClassCode(MultiResponse.class).intValue());

    // coprocessor execution
    assertEquals(68,HbaseObjectWritable.getClassCode(Exec.class).intValue());
    assertEquals(69,HbaseObjectWritable.getClassCode(Increment.class).intValue());

    assertEquals(70,HbaseObjectWritable.getClassCode(KeyOnlyFilter.class).intValue());

    // serializable
    assertEquals(71,HbaseObjectWritable.getClassCode(Serializable.class).intValue());
    assertEquals(72,HbaseObjectWritable.getClassCode(RandomRowFilter.class).intValue());
    assertEquals(73,HbaseObjectWritable.getClassCode(CompareOp.class).intValue());
    assertEquals(74,HbaseObjectWritable.getClassCode(ColumnRangeFilter.class).intValue());
    assertEquals(75,HbaseObjectWritable.getClassCode(HServerLoad.class).intValue());
    assertEquals(76,HbaseObjectWritable.getClassCode(RegionOpeningState.class).intValue());
    assertEquals(77,HbaseObjectWritable.getClassCode(HTableDescriptor[].class).intValue());
    assertEquals(78,HbaseObjectWritable.getClassCode(Append.class).intValue());
    assertEquals(79,HbaseObjectWritable.getClassCode(RowMutations.class).intValue());
    assertEquals(80,HbaseObjectWritable.getClassCode(Message.class).intValue());

    assertEquals(81,HbaseObjectWritable.getClassCode(Array.class).intValue());
  }

  /**
   * This test verifies that additional objects have not been added to the end of the list.
   * If you are legitimately adding objects, this test will need to be updated, but see the
   * note on the test above. 
   */
  public void testGetNextObjectCode(){
    assertEquals(83,HbaseObjectWritable.getNextClassCode());
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

