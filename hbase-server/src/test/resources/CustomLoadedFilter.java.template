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
import java.util.Objects;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * Just wraps around a delegate, the only goal here is to create a filter which doesn't exist
 * in org.apache.hadoop.hbase.filter so it doesn't get automatically loaded at startup. We can
 * pass it into the DynamicClassLoader to prove that (de)serialization works.
 */
public class CustomLoadedFilter${suffix} extends FilterBase {

  private final PrefixFilter delegate;

  public CustomLoadedFilter${suffix}(PrefixFilter delegate) {
    this.delegate = delegate;
  }

  @Override
  public byte[] toByteArray() {
    FilterProtos.PrefixFilter.Builder builder = FilterProtos.PrefixFilter.newBuilder();
    if (this.delegate.getPrefix() != null) builder.setPrefix(UnsafeByteOperations.unsafeWrap(this.delegate.getPrefix()));
    return builder.build().toByteArray();
  }

  public static CustomLoadedFilter${suffix} parseFrom(final byte[] pbBytes) throws
    DeserializationException {
    FilterProtos.PrefixFilter proto;
    try {
      proto = FilterProtos.PrefixFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new CustomLoadedFilter${suffix}(new PrefixFilter(proto.hasPrefix() ? proto.getPrefix().toByteArray() : null));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CustomLoadedFilter${suffix} that = (CustomLoadedFilter${suffix}) o;
    return Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate);
  }
}
