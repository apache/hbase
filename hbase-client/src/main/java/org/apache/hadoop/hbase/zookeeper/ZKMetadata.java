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
package org.apache.hadoop.hbase.zookeeper;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The metadata append to the start of data on zookeeper.
 */
@InterfaceAudience.Private
public class ZKMetadata {

  private ZKMetadata() {
  }

  // The metadata attached to each piece of data has the format:
  // <magic> 1-byte constant
  // <id length> 4-byte big-endian integer (length of next field)
  // <id> identifier corresponding uniquely to this process
  // It is prepended to the data supplied by the user.

  // the magic number is to be backward compatible
  private static final byte MAGIC = (byte) 0XFF;
  private static final int MAGIC_SIZE = Bytes.SIZEOF_BYTE;
  private static final int ID_LENGTH_OFFSET = MAGIC_SIZE;
  private static final int ID_LENGTH_SIZE = Bytes.SIZEOF_INT;

  public static byte[] appendMetaData(byte[] id, byte[] data) {
    if (data == null || data.length == 0) {
      return data;
    }
    byte[] salt = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
    int idLength = id.length + salt.length;
    byte[] newData = new byte[MAGIC_SIZE + ID_LENGTH_SIZE + idLength + data.length];
    int pos = 0;
    pos = Bytes.putByte(newData, pos, MAGIC);
    pos = Bytes.putInt(newData, pos, idLength);
    pos = Bytes.putBytes(newData, pos, id, 0, id.length);
    pos = Bytes.putBytes(newData, pos, salt, 0, salt.length);
    pos = Bytes.putBytes(newData, pos, data, 0, data.length);
    return newData;
  }

  public static byte[] removeMetaData(byte[] data) {
    if (data == null || data.length == 0) {
      return data;
    }
    // check the magic data; to be backward compatible
    byte magic = data[0];
    if (magic != MAGIC) {
      return data;
    }

    int idLength = Bytes.toInt(data, ID_LENGTH_OFFSET);
    int dataLength = data.length - MAGIC_SIZE - ID_LENGTH_SIZE - idLength;
    int dataOffset = MAGIC_SIZE + ID_LENGTH_SIZE + idLength;

    byte[] newData = new byte[dataLength];
    System.arraycopy(data, dataOffset, newData, 0, dataLength);
    return newData;
  }
}
