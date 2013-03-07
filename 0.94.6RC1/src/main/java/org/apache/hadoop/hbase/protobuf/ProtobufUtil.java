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
package org.apache.hadoop.hbase.protobuf;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Protobufs utility.
 */
@SuppressWarnings("deprecation")
public final class ProtobufUtil {

  private ProtobufUtil() {
  }

  /**
   * Magic we put ahead of a serialized protobuf message.
   * For example, all znode content is protobuf messages with the below magic
   * for preamble.
   */
  public static final byte [] PB_MAGIC = new byte [] {'P', 'B', 'U', 'F'};
  private static final String PB_MAGIC_STR = Bytes.toString(PB_MAGIC);

  /**
   * Prepend the passed bytes with four bytes of magic, {@link #PB_MAGIC}, to flag what
   * follows as a protobuf in hbase.  Prepend these bytes to all content written to znodes, etc.
   * @param bytes Bytes to decorate
   * @return The passed <code>bytes</codes> with magic prepended (Creates a new
   * byte array that is <code>bytes.length</code> plus {@link #PB_MAGIC}.length.
   */
  public static byte [] prependPBMagic(final byte [] bytes) {
    return Bytes.add(PB_MAGIC, bytes);
  }

  /**
   * @param bytes Bytes to check.
   * @return True if passed <code>bytes</code> has {@link #PB_MAGIC} for a prefix.
   */
  public static boolean isPBMagicPrefix(final byte [] bytes) {
    if (bytes == null || bytes.length < PB_MAGIC.length) return false;
    return Bytes.compareTo(PB_MAGIC, 0, PB_MAGIC.length, bytes, 0, PB_MAGIC.length) == 0;
  }

  /**
   * @return Length of {@link #PB_MAGIC}
   */
  public static int lengthOfPBMagic() {
    return PB_MAGIC.length;
  }
}
