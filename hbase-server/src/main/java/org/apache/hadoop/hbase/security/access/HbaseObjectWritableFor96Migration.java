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

package org.apache.hadoop.hbase.security.access;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.ClusterStatus;
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
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
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
import org.apache.hadoop.hbase.io.WritableWithSize;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ProtoUtil;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;

import com.google.protobuf.Message;
import com.google.protobuf.RpcController;

/**
 * <p>This is a customized version of the polymorphic hadoop
 * {@link ObjectWritable}.  It removes UTF8 (HADOOP-414).
 * Using {@link Text} intead of UTF-8 saves ~2% CPU between reading and writing
 * objects running a short sequentialWrite Performance Evaluation test just in
 * ObjectWritable alone; more when we're doing randomRead-ing.  Other
 * optimizations include our passing codes for classes instead of the
 * actual class names themselves.  This makes it so this class needs amendment
 * if non-Writable classes are introduced -- if passed a Writable for which we
 * have no code, we just do the old-school passing of the class name, etc. --
 * but passing codes the  savings are large particularly when cell
 * data is small (If < a couple of kilobytes, the encoding/decoding of class
 * name and reflection to instantiate class was costing in excess of the cell
 * handling).
 * @deprecated This class is needed migrating TablePermissions written with
 * Writables.  It is needed to read old permissions written pre-0.96.  This
 * class is to be removed after HBase 0.96 ships since then all permissions
 * will have been migrated and written with protobufs.
 */
@Deprecated
@InterfaceAudience.Private
class HbaseObjectWritableFor96Migration implements Writable, WritableWithSize, Configurable {
  private final static Log LOG = LogFactory.getLog(HbaseObjectWritableFor96Migration.class);

  // Here we maintain two static maps of classes to code and vice versa.
  // Add new classes+codes as wanted or figure way to auto-generate these
  // maps.
  static final Map<Integer, Class<?>> CODE_TO_CLASS =
    new HashMap<Integer, Class<?>>();
  static final Map<Class<?>, Integer> CLASS_TO_CODE =
    new HashMap<Class<?>, Integer>();
  // Special code that means 'not-encoded'; in this case we do old school
  // sending of the class name using reflection, etc.
  private static final byte NOT_ENCODED = 0;
  //Generic array means that the array type is not one of the pre-defined arrays
  //in the CLASS_TO_CODE map, but we have to still encode the array since it's
  //elements are serializable by this class.
  private static final int GENERIC_ARRAY_CODE;
  private static final int NEXT_CLASS_CODE;
  static {
    ////////////////////////////////////////////////////////////////////////////
    // WARNING: Please do not insert, remove or swap any line in this static  //
    // block.  Doing so would change or shift all the codes used to serialize //
    // objects, which makes backwards compatibility very hard for clients.    //
    // New codes should always be added at the end. Code removal is           //
    // discouraged because code is a short now.                               //
    ////////////////////////////////////////////////////////////////////////////

    int code = NOT_ENCODED + 1;
    // Primitive types.
    addToMap(Boolean.TYPE, code++);
    addToMap(Byte.TYPE, code++);
    addToMap(Character.TYPE, code++);
    addToMap(Short.TYPE, code++);
    addToMap(Integer.TYPE, code++);
    addToMap(Long.TYPE, code++);
    addToMap(Float.TYPE, code++);
    addToMap(Double.TYPE, code++);
    addToMap(Void.TYPE, code++);

    // Other java types
    addToMap(String.class, code++);
    addToMap(byte [].class, code++);
    addToMap(byte [][].class, code++);

    // Hadoop types
    addToMap(Text.class, code++);
    addToMap(Writable.class, code++);
    addToMap(Writable [].class, code++);
    code++; // Removed
    addToMap(NullInstance.class, code++);

    // Hbase types
    addToMap(HColumnDescriptor.class, code++);
    addToMap(HConstants.Modify.class, code++);

    // We used to have a class named HMsg but its been removed.  Rather than
    // just axe it, use following random Integer class -- we just chose any
    // class from java.lang -- instead just so codes that follow stay
    // in same relative place.
    addToMap(Integer.class, code++);
    addToMap(Integer[].class, code++);

    //HRegion shouldn't be pushed across the wire.
    code++; //addToMap(HRegion.class, code++);
    code++; //addToMap(HRegion[].class, code++);

    addToMap(HRegionInfo.class, code++);
    addToMap(HRegionInfo[].class, code++);
    code++; // Removed
    code++; // Removed
    addToMap(HTableDescriptor.class, code++);
    addToMap(MapWritable.class, code++);

    //
    // HBASE-880
    //
    addToMap(ClusterStatus.class, code++);
    addToMap(Delete.class, code++);
    addToMap(Get.class, code++);
    addToMap(KeyValue.class, code++);
    addToMap(KeyValue[].class, code++);
    addToMap(Put.class, code++);
    addToMap(Put[].class, code++);
    addToMap(Result.class, code++);
    addToMap(Result[].class, code++);
    addToMap(Scan.class, code++);

    addToMap(WhileMatchFilter.class, code++);
    addToMap(PrefixFilter.class, code++);
    addToMap(PageFilter.class, code++);
    addToMap(InclusiveStopFilter.class, code++);
    addToMap(ColumnCountGetFilter.class, code++);
    addToMap(SingleColumnValueFilter.class, code++);
    addToMap(SingleColumnValueExcludeFilter.class, code++);
    addToMap(BinaryComparator.class, code++);
    addToMap(BitComparator.class, code++);
    addToMap(CompareFilter.class, code++);
    addToMap(RowFilter.class, code++);
    addToMap(ValueFilter.class, code++);
    addToMap(QualifierFilter.class, code++);
    addToMap(SkipFilter.class, code++);
    addToMap(ByteArrayComparable.class, code++);
    addToMap(FirstKeyOnlyFilter.class, code++);
    addToMap(DependentColumnFilter.class, code++);

    addToMap(Delete [].class, code++);

    addToMap(Entry.class, code++);
    addToMap(Entry[].class, code++);
    addToMap(HLogKey.class, code++);

    addToMap(List.class, code++);

    addToMap(NavigableSet.class, code++);
    addToMap(ColumnPrefixFilter.class, code++);

    // Multi
    addToMap(Row.class, code++);
    addToMap(Action.class, code++);
    addToMap(MultiAction.class, code++);
    addToMap(MultiResponse.class, code++);

    // coprocessor execution
    // Exec no longer exists --> addToMap(Exec.class, code++);
    code++;
    addToMap(Increment.class, code++);

    addToMap(KeyOnlyFilter.class, code++);

    // serializable
    addToMap(Serializable.class, code++);

    addToMap(RandomRowFilter.class, code++);

    addToMap(CompareOp.class, code++);

    addToMap(ColumnRangeFilter.class, code++);

    // HServerLoad no longer exists; increase code so other classes stay the same.
    code++;
    //addToMap(HServerLoad.class, code++);

    addToMap(RegionOpeningState.class, code++);

    addToMap(HTableDescriptor[].class, code++);

    addToMap(Append.class, code++);

    addToMap(RowMutations.class, code++);

    addToMap(Message.class, code++);

    //java.lang.reflect.Array is a placeholder for arrays not defined above
    GENERIC_ARRAY_CODE = code++;
    addToMap(Array.class, GENERIC_ARRAY_CODE);

    addToMap(RpcController.class, code++);

    // make sure that this is the last statement in this static block
    NEXT_CLASS_CODE = code;
  }

  private Class<?> declaredClass;
  private Object instance;
  private Configuration conf;

  /** default constructor for writable */
  HbaseObjectWritableFor96Migration() {
    super();
  }

  /**
   * @param instance
   */
  HbaseObjectWritableFor96Migration(Object instance) {
    set(instance);
  }

  /**
   * @param declaredClass
   * @param instance
   */
  HbaseObjectWritableFor96Migration(Class<?> declaredClass, Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** @return the instance, or null if none. */
  Object get() { return instance; }

  /** @return the class this is meant to be. */
  Class<?> getDeclaredClass() { return declaredClass; }

  /**
   * Reset the instance.
   * @param instance
   */
  void set(Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "OW[class=" + declaredClass + ",value=" + instance + "]";
  }


  public void readFields(DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }

  public void write(DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass, conf);
  }

  public long getWritableSize() {
    return getWritableSize(instance, declaredClass, conf);
  }

  private static class NullInstance extends Configured implements Writable {
    Class<?> declaredClass;
    /** default constructor for writable */
    @SuppressWarnings("unused")
    public NullInstance() { super(null); }

    /**
     * @param declaredClass
     * @param conf
     */
    public NullInstance(Class<?> declaredClass, Configuration conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }

    public void readFields(DataInput in) throws IOException {
      this.declaredClass = CODE_TO_CLASS.get(WritableUtils.readVInt(in));
    }

    public void write(DataOutput out) throws IOException {
      writeClassCode(out, this.declaredClass);
    }
  }

  static Integer getClassCode(final Class<?> c)
  throws IOException {
    Integer code = CLASS_TO_CODE.get(c);
    if (code == null ) {
      if (List.class.isAssignableFrom(c)) {
        code = CLASS_TO_CODE.get(List.class);
      } else if (Writable.class.isAssignableFrom(c)) {
        code = CLASS_TO_CODE.get(Writable.class);
      } else if (c.isArray()) {
        code = CLASS_TO_CODE.get(Array.class);
      } else if (Message.class.isAssignableFrom(c)) {
        code = CLASS_TO_CODE.get(Message.class);
      } else if (Serializable.class.isAssignableFrom(c)){
        code = CLASS_TO_CODE.get(Serializable.class);
      } else if (Scan.class.isAssignableFrom(c)) {
        code = CLASS_TO_CODE.get(Scan.class);
      }
    }
    return code;
  }

  /**
   * @return the next object code in the list.  Used in testing to verify that additional fields are not added 
   */
  static int getNextClassCode(){
    return NEXT_CLASS_CODE;
  }

  /**
   * Write out the code for passed Class.
   * @param out
   * @param c
   * @throws IOException
   */
  static void writeClassCode(final DataOutput out, final Class<?> c)
      throws IOException {
    Integer code = getClassCode(c);

    if (code == null) {
      LOG.error("Unsupported type " + c);
      StackTraceElement[] els = new Exception().getStackTrace();
      for(StackTraceElement elem : els) {
        LOG.error(elem.getMethodName());
      }
      throw new UnsupportedOperationException("No code for unexpected " + c);
    }
    WritableUtils.writeVInt(out, code);
  }

  static long getWritableSize(Object instance, Class declaredClass,
                                     Configuration conf) {
    return 0L; // no hint is the default.
  }
  /**
   * Write a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding.
   * @param out
   * @param instance
   * @param declaredClass
   * @param conf
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  static void writeObject(DataOutput out, Object instance,
                                 Class declaredClass,
                                 Configuration conf)
  throws IOException {

    Object instanceObj = instance;
    Class declClass = declaredClass;

    if (instanceObj == null) {                       // null
      instanceObj = new NullInstance(declClass, conf);
      declClass = Writable.class;
    }
    writeClassCode(out, declClass);
    if (declClass.isArray()) {                // array
      // If bytearray, just dump it out -- avoid the recursion and
      // byte-at-a-time we were previously doing.
      if (declClass.equals(byte [].class)) {
        Bytes.writeByteArray(out, (byte [])instanceObj);
      } else {
        //if it is a Generic array, write the element's type
        if (getClassCode(declaredClass) == GENERIC_ARRAY_CODE) {
          Class<?> componentType = declaredClass.getComponentType();
          writeClass(out, componentType);
        }

        int length = Array.getLength(instanceObj);
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
          Object item = Array.get(instanceObj, i);
          writeObject(out, item,
                    item.getClass(), conf);
        }
      }
    } else if (List.class.isAssignableFrom(declClass)) {
      List list = (List)instanceObj;
      int length = list.size();
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        Object elem = list.get(i);
        writeObject(out, elem,
                  elem == null ? Writable.class : elem.getClass(), conf);
      }
    } else if (declClass == String.class) {   // String
      Text.writeString(out, (String)instanceObj);
    } else if (declClass.isPrimitive()) {     // primitive type
      if (declClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((Boolean)instanceObj).booleanValue());
      } else if (declClass == Character.TYPE) { // char
        out.writeChar(((Character)instanceObj).charValue());
      } else if (declClass == Byte.TYPE) {    // byte
        out.writeByte(((Byte)instanceObj).byteValue());
      } else if (declClass == Short.TYPE) {   // short
        out.writeShort(((Short)instanceObj).shortValue());
      } else if (declClass == Integer.TYPE) { // int
        out.writeInt(((Integer)instanceObj).intValue());
      } else if (declClass == Long.TYPE) {    // long
        out.writeLong(((Long)instanceObj).longValue());
      } else if (declClass == Float.TYPE) {   // float
        out.writeFloat(((Float)instanceObj).floatValue());
      } else if (declClass == Double.TYPE) {  // double
        out.writeDouble(((Double)instanceObj).doubleValue());
      } else if (declClass == Void.TYPE) {    // void
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declClass);
      }
    } else if (declClass.isEnum()) {         // enum
      Text.writeString(out, ((Enum)instanceObj).name());
    } else if (Message.class.isAssignableFrom(declaredClass)) {
      Text.writeString(out, instanceObj.getClass().getName());
      ((Message)instance).writeDelimitedTo(
          DataOutputOutputStream.constructOutputStream(out));
    } else if (Writable.class.isAssignableFrom(declClass)) { // Writable
      Class <?> c = instanceObj.getClass();
      Integer code = CLASS_TO_CODE.get(c);
      if (code == null) {
        out.writeByte(NOT_ENCODED);
        Text.writeString(out, c.getName());
      } else {
        writeClassCode(out, c);
      }
      ((Writable)instanceObj).write(out);
    } else if (Serializable.class.isAssignableFrom(declClass)) {
      Class <?> c = instanceObj.getClass();
      Integer code = CLASS_TO_CODE.get(c);
      if (code == null) {
        out.writeByte(NOT_ENCODED);
        Text.writeString(out, c.getName());
      } else {
        writeClassCode(out, c);
      }
      ByteArrayOutputStream bos = null;
      ObjectOutputStream oos = null;
      try{
        bos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(bos);
        oos.writeObject(instanceObj);
        byte[] value = bos.toByteArray();
        out.writeInt(value.length);
        out.write(value);
      } finally {
        if(bos!=null) bos.close();
        if(oos!=null) oos.close();
      }
    } else if (Scan.class.isAssignableFrom(declClass)) {
      Scan scan = (Scan)instanceObj;
      byte [] scanBytes = ProtobufUtil.toScan(scan).toByteArray();
      out.writeInt(scanBytes.length);
      out.write(scanBytes);
    } else if (Entry.class.isAssignableFrom(declClass)) {
      // Entry is no longer Writable, maintain compatible serialization.
      // Writables write their exact runtime class
      Class <?> c = instanceObj.getClass();
      Integer code = CLASS_TO_CODE.get(c);
      if (code == null) {
        out.writeByte(NOT_ENCODED);
        Text.writeString(out, c.getName());
      } else {
        writeClassCode(out, c);
      }
      final Entry entry = (Entry)instanceObj;
      // We only support legacy HLogKey
      WALKey key = entry.getKey();
      if (!(key instanceof HLogKey)) {
        throw new IOException("Can't write Entry '" + instanceObj + "' due to key class '" +
            key.getClass() + "'");
      }
      ((HLogKey)key).write(out);
      entry.getEdit().write(out);
    } else {
      throw new IOException("Can't write: "+instanceObj+" as "+declClass);
    }
  }

  /** Writes the encoded class code as defined in CLASS_TO_CODE, or
   * the whole class name if not defined in the mapping.
   */
  static void writeClass(DataOutput out, Class<?> c) throws IOException {
    Integer code = CLASS_TO_CODE.get(c);
    if (code == null) {
      WritableUtils.writeVInt(out, NOT_ENCODED);
      Text.writeString(out, c.getName());
    } else {
      WritableUtils.writeVInt(out, code);
    }
  }

  /** Reads and returns the class as written by {@link #writeClass(DataOutput, Class)} */
  static Class<?> readClass(Configuration conf, DataInput in) throws IOException {
    Class<?> instanceClass = null;
    int b = (byte)WritableUtils.readVInt(in);
    if (b == NOT_ENCODED) {
      String className = Text.readString(in);
      try {
        instanceClass = getClassByName(conf, className);
      } catch (ClassNotFoundException e) {
        LOG.error("Can't find class " + className, e);
        throw new IOException("Can't find class " + className, e);
      }
    } else {
      instanceClass = CODE_TO_CLASS.get(b);
    }
    return instanceClass;
  }

  /**
   * Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding.
   * @param in
   * @param conf
   * @return the object
   * @throws IOException
   */
  static Object readObject(DataInput in, Configuration conf)
    throws IOException {
    return readObject(in, null, conf);
  }

  /**
   * Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding.
   * @param in
   * @param objectWritable
   * @param conf
   * @return the object
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  static Object readObject(DataInput in,
      HbaseObjectWritableFor96Migration objectWritable, Configuration conf)
  throws IOException {
    Class<?> declaredClass = CODE_TO_CLASS.get(WritableUtils.readVInt(in));
    Object instance;
    if (declaredClass.isPrimitive()) {            // primitive types
      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = Character.valueOf(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = Byte.valueOf(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = Short.valueOf(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = Integer.valueOf(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = Long.valueOf(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = Float.valueOf(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = Double.valueOf(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isArray()) {              // array
      if (declaredClass.equals(byte [].class)) {
        instance = Bytes.readByteArray(in);
      } else {
        int length = in.readInt();
        instance = Array.newInstance(declaredClass.getComponentType(), length);
        for (int i = 0; i < length; i++) {
          Array.set(instance, i, readObject(in, conf));
        }
      }
    } else if (declaredClass.equals(Array.class)) { //an array not declared in CLASS_TO_CODE
      Class<?> componentType = readClass(conf, in);
      int length = in.readInt();
      instance = Array.newInstance(componentType, length);
      for (int i = 0; i < length; i++) {
        Array.set(instance, i, readObject(in, conf));
      }
    } else if (List.class.isAssignableFrom(declaredClass)) {            // List
      int length = in.readInt();
      instance = new ArrayList(length);
      for (int i = 0; i < length; i++) {
        ((ArrayList)instance).add(readObject(in, conf));
      }
    } else if (declaredClass == String.class) {        // String
      instance = Text.readString(in);
    } else if (declaredClass.isEnum()) {         // enum
      instance = Enum.valueOf((Class<? extends Enum>) declaredClass,
        Text.readString(in));
    } else if (declaredClass == Message.class) {
      String className = Text.readString(in);
      try {
        declaredClass = getClassByName(conf, className);
        instance = tryInstantiateProtobuf(declaredClass, in);
      } catch (ClassNotFoundException e) {
        LOG.error("Can't find class " + className, e);
        throw new IOException("Can't find class " + className, e);
      }
    } else if (Scan.class.isAssignableFrom(declaredClass)) {
      int length = in.readInt();
      byte [] scanBytes = new byte[length];
      in.readFully(scanBytes);
      ClientProtos.Scan.Builder scanProto = ClientProtos.Scan.newBuilder();
      instance = ProtobufUtil.toScan(scanProto.mergeFrom(scanBytes).build());
    } else {                                      // Writable or Serializable
      Class instanceClass = null;
      int b = (byte)WritableUtils.readVInt(in);
      if (b == NOT_ENCODED) {
        String className = Text.readString(in);
        if ("org.apache.hadoop.hbase.regionserver.wal.HLog$Entry".equals(className)) {
          className = Entry.class.getName();
        }
        try {
          instanceClass = getClassByName(conf, className);
        } catch (ClassNotFoundException e) {
          LOG.error("Can't find class " + className, e);
          throw new IOException("Can't find class " + className, e);
        }
      } else {
        instanceClass = CODE_TO_CLASS.get(b);
      }
      if(Writable.class.isAssignableFrom(instanceClass)){
        Writable writable = WritableFactories.newInstance(instanceClass, conf);
        try {
          writable.readFields(in);
        } catch (Exception e) {
          LOG.error("Error in readFields", e);
          throw new IOException("Error in readFields" , e);
        }
        instance = writable;
        if (instanceClass == NullInstance.class) {  // null
          declaredClass = ((NullInstance)instance).declaredClass;
          instance = null;
        }
      } else if (Entry.class.isAssignableFrom(instanceClass)) {
        // Entry stopped being Writable; maintain serialization support.
        final HLogKey key = new HLogKey();
        final WALEdit edit = new WALEdit();
        key.readFields(in);
        edit.readFields(in);
        instance = new Entry(key, edit);
      } else {
        int length = in.readInt();
        byte[] objectBytes = new byte[length];
        in.readFully(objectBytes);
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
          bis = new ByteArrayInputStream(objectBytes);
          ois = new ObjectInputStream(bis);
          instance = ois.readObject();
        } catch (ClassNotFoundException e) {
          LOG.error("Class not found when attempting to deserialize object", e);
          throw new IOException("Class not found when attempting to " +
              "deserialize object", e);
        } finally {
          if(bis!=null) bis.close();
          if(ois!=null) ois.close();
        }
      }
    }
    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }
    return instance;
  }

  /**
   * Try to instantiate a protocol buffer of the given message class
   * from the given input stream.
   *
   * @param protoClass the class of the generated protocol buffer
   * @param dataIn the input stream to read from
   * @return the instantiated Message instance
   * @throws IOException if an IO problem occurs
   */
  static Message tryInstantiateProtobuf(
      Class<?> protoClass,
      DataInput dataIn) throws IOException {

    try {
      if (dataIn instanceof InputStream) {
        // We can use the built-in parseDelimitedFrom and not have to re-copy
        // the data
        Method parseMethod = getStaticProtobufMethod(protoClass,
            "parseDelimitedFrom", InputStream.class);
        return (Message)parseMethod.invoke(null, (InputStream)dataIn);
      } else {
        // Have to read it into a buffer first, since protobuf doesn't deal
        // with the DataInput interface directly.

        // Read the size delimiter that writeDelimitedTo writes
        int size = ProtoUtil.readRawVarint32(dataIn);
        if (size < 0) {
          throw new IOException("Invalid size: " + size);
        }

        byte[] data = new byte[size];
        dataIn.readFully(data);
        Method parseMethod = getStaticProtobufMethod(protoClass,
            "parseFrom", byte[].class);
        return (Message)parseMethod.invoke(null, data);
      }
    } catch (InvocationTargetException e) {

      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    } catch (IllegalAccessException iae) {
      throw new AssertionError("Could not access parse method in " +
          protoClass);
    }
  }

  static Method getStaticProtobufMethod(Class<?> declaredClass, String method,
      Class<?> ... args) {

    try {
      return declaredClass.getMethod(method, args);
    } catch (Exception e) {
      // This is a bug in Hadoop - protobufs should all have this static method
      throw new AssertionError("Protocol buffer class " + declaredClass +
          " does not have an accessible parseFrom(InputStream) method!");
    }
  }

  @SuppressWarnings("unchecked")
  private static Class getClassByName(Configuration conf, String className)
  throws ClassNotFoundException {
    if(conf != null) {
      return conf.getClassByName(className);
    }
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if(cl == null) {
      cl = HbaseObjectWritableFor96Migration.class.getClassLoader();
    }
    return Class.forName(className, true, cl);
  }

  private static void addToMap(final Class<?> clazz, final int code) {
    CLASS_TO_CODE.put(clazz, code);
    CODE_TO_CLASS.put(code, clazz);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
}
