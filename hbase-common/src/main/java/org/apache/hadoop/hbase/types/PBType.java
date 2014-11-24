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
package org.apache.hadoop.hbase.types;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;

/**
 * A base-class for {@link DataType} implementations backed by protobuf. See
 * {@code PBKeyValue} in {@code hbase-examples} module.
 */
public abstract class PBType<T extends Message> implements DataType<T> {
  @Override
  public boolean isOrderPreserving() {
    return false;
  }

  @Override
  public Order getOrder() {
    return null;
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public boolean isSkippable() {
    return true;
  }

  @Override
  public int encodedLength(T val) {
    return val.getSerializedSize();
  }

  /**
   * Create a {@link CodedInputStream} from a {@link PositionedByteRange}. Be sure to update
   * {@code src}'s position after consuming from the stream.
   * <p>For example:
   * <pre>
   * Foo.Builder builder = ...
   * CodedInputStream is = inputStreamFromByteRange(src);
   * Foo ret = builder.mergeFrom(is).build();
   * src.setPosition(src.getPosition() + is.getTotalBytesRead());
   * </pre>
   */
  public static CodedInputStream inputStreamFromByteRange(PositionedByteRange src) {
    return CodedInputStream.newInstance(
      src.getBytes(),
      src.getOffset() + src.getPosition(),
      src.getRemaining());
  }

  /**
   * Create a {@link CodedOutputStream} from a {@link PositionedByteRange}. Be sure to update
   * {@code dst}'s position after writing to the stream.
   * <p>For example:
   * <pre>
   * CodedOutputStream os = outputStreamFromByteRange(dst);
   * int before = os.spaceLeft(), after, written;
   * val.writeTo(os);
   * after = os.spaceLeft();
   * written = before - after;
   * dst.setPosition(dst.getPosition() + written);
   * </pre>
   */
  public static CodedOutputStream outputStreamFromByteRange(PositionedByteRange dst) {
    return CodedOutputStream.newInstance(
      dst.getBytes(),
      dst.getOffset() + dst.getPosition(),
      dst.getRemaining()
    );
  }
}
