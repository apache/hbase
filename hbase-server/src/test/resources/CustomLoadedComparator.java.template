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
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.TestFilterSerialization;

/**
 * Just wraps around a delegate, the only goal here is to create a Comparable which doesn't exist
 * in org.apache.hadoop.hbase.filter so it doesn't get automatically loaded at startup. We can
 * pass it into the DynamicClassLoader to prove that (de)serialization works.
 */
public class CustomLoadedComparator${suffix} extends ByteArrayComparable {

  private final BinaryComparator delegate;

  public CustomLoadedComparator${suffix}(BinaryComparator delegate) {
    super(delegate.getValue());
    this.delegate = delegate;
  }

  @Override
  public byte[] toByteArray() {
    return delegate.toByteArray();
  }

  public static CustomLoadedComparator${suffix} parseFrom(final byte[] pbBytes) throws
    DeserializationException {
    return new CustomLoadedComparator${suffix}(BinaryComparator.parseFrom(pbBytes));
  }

  @Override public int compareTo(byte[] value, int offset, int length) {
    return delegate.compareTo(value, offset, length);
  }

  @Override public byte[] getValue() {
    return delegate.getValue();
  }

  @Override public int compareTo(byte[] value) {
    return delegate.compareTo(value);
  }

  @Override public int hashCode() {
    return delegate.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
