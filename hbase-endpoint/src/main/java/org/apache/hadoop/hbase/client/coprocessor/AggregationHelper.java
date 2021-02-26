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
package org.apache.hadoop.hbase.client.coprocessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class for constructing aggregation request and response.
 */
@InterfaceAudience.Private
public final class AggregationHelper {
  private AggregationHelper() {}

  /**
   * @param scan the HBase scan object to use to read data from HBase
   * @param canFamilyBeAbsent whether column family can be absent in familyMap of scan
   */
  private static void validateParameters(Scan scan, boolean canFamilyBeAbsent) throws IOException {
    if (scan == null
        || (Bytes.equals(scan.getStartRow(), scan.getStopRow())
            && !Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
        || ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0)
            && !Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
      throw new IOException("Agg client Exception: Startrow should be smaller than Stoprow");
    } else if (!canFamilyBeAbsent) {
      if (scan.getFamilyMap().size() != 1) {
        throw new IOException("There must be only one family.");
      }
    }
  }

  static <R, S, P extends Message, Q extends Message, T extends Message> AggregateRequest
      validateArgAndGetPB(Scan scan, ColumnInterpreter<R, S, P, Q, T> ci, boolean canFamilyBeAbsent)
          throws IOException {
    validateParameters(scan, canFamilyBeAbsent);
    final AggregateRequest.Builder requestBuilder = AggregateRequest.newBuilder();
    requestBuilder.setInterpreterClassName(ci.getClass().getCanonicalName());
    P columnInterpreterSpecificData = ci.getRequestData();
    if (columnInterpreterSpecificData != null) {
      requestBuilder.setInterpreterSpecificBytes(columnInterpreterSpecificData.toByteString());
    }
    requestBuilder.setScan(ProtobufUtil.toScan(scan));
    return requestBuilder.build();
  }

  /**
   * Get an instance of the argument type declared in a class's signature. The argument type is
   * assumed to be a PB Message subclass, and the instance is created using parseFrom method on the
   * passed ByteString.
   * @param runtimeClass the runtime type of the class
   * @param position the position of the argument in the class declaration
   * @param b the ByteString which should be parsed to get the instance created
   * @return the instance
   * @throws IOException Either we couldn't instantiate the method object, or "parseFrom" failed.
   */
  @SuppressWarnings("unchecked")
  // Used server-side too by Aggregation Coprocesor Endpoint. Undo this interdependence. TODO.
  public static <T extends Message> T getParsedGenericInstance(Class<?> runtimeClass, int position,
      ByteString b) throws IOException {
    Type type = runtimeClass.getGenericSuperclass();
    Type argType = ((ParameterizedType) type).getActualTypeArguments()[position];
    Class<T> classType = (Class<T>) argType;
    T inst;
    try {
      Method m = classType.getMethod("parseFrom", ByteString.class);
      inst = (T) m.invoke(null, b);
      return inst;
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (NoSuchMethodException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (InvocationTargetException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }
}
