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
package org.apache.hadoop.hbase.io.encoding;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Provide access to all index block encoding algorithms. All of the algorithms are required to have
 * unique id which should <b>NEVER</b> be changed. If you want to add a new algorithm/version,
 * assign it a new id. Announce the new id in the HBase mailing list to prevent collisions.
 */
@InterfaceAudience.Public
public enum IndexBlockEncoding {

  /** Disable index block encoding. */
  NONE(0, null),
  // id 1 is reserved for the PREFIX_TREE algorithm to be added later
  PREFIX_TREE(1, null);

  private final short id;
  private final byte[] idInBytes;
  private final String encoderCls;

  public static final int ID_SIZE = Bytes.SIZEOF_SHORT;

  /** Maps data block encoding ids to enum instances. */
  private static IndexBlockEncoding[] idArray = new IndexBlockEncoding[Byte.MAX_VALUE + 1];

  static {
    for (IndexBlockEncoding algo : values()) {
      if (idArray[algo.id] != null) {
        throw new RuntimeException(
          String.format("Two data block encoder algorithms '%s' and '%s' have " + "the same id %d",
            idArray[algo.id].toString(), algo.toString(), (int) algo.id));
      }
      idArray[algo.id] = algo;
    }
  }

  private IndexBlockEncoding(int id, String encoderClsName) {
    if (id < 0 || id > Byte.MAX_VALUE) {
      throw new AssertionError("Data block encoding algorithm id is out of range: " + id);
    }
    this.id = (short) id;
    this.idInBytes = Bytes.toBytes(this.id);
    if (idInBytes.length != ID_SIZE) {
      // White this may seem redundant, if we accidentally serialize
      // the id as e.g. an int instead of a short, all encoders will break.
      throw new RuntimeException("Unexpected length of encoder ID byte " + "representation: "
        + Bytes.toStringBinary(idInBytes));
    }
    this.encoderCls = encoderClsName;
  }

  /** Returns name converted to bytes. */
  public byte[] getNameInBytes() {
    return Bytes.toBytes(toString());
  }

  /** Returns The id of a data block encoder. */
  public short getId() {
    return id;
  }

  /**
   * Writes id in bytes.
   * @param stream where the id should be written.
   */
  public void writeIdInBytes(OutputStream stream) throws IOException {
    stream.write(idInBytes);
  }

  /**
   * Writes id bytes to the given array starting from offset.
   * @param dest   output array
   * @param offset starting offset of the output array
   */
  public void writeIdInBytes(byte[] dest, int offset) throws IOException {
    System.arraycopy(idInBytes, 0, dest, offset, ID_SIZE);
  }

  /**
   * Find and return the name of data block encoder for the given id.
   * @param encoderId id of data block encoder
   * @return name, same as used in options in column family
   */
  public static String getNameFromId(short encoderId) {
    return getEncodingById(encoderId).toString();
  }

  public static IndexBlockEncoding getEncodingById(short indexBlockEncodingId) {
    IndexBlockEncoding algorithm = null;
    if (indexBlockEncodingId >= 0 && indexBlockEncodingId <= Byte.MAX_VALUE) {
      algorithm = idArray[indexBlockEncodingId];
    }
    if (algorithm == null) {
      throw new IllegalArgumentException(String
        .format("There is no index block encoder for given id '%d'", (int) indexBlockEncodingId));
    }
    return algorithm;
  }

}
