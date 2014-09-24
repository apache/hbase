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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * DO NOT USE. This class is deprecated and should only be used in pre-PB WAL.
 * 
 * Compression class for {@link KeyValue}s written to the WAL. This is not
 * synchronized, so synchronization should be handled outside.
 * 
 * Class only compresses and uncompresses row keys, family names, and the
 * qualifier. More may be added depending on use patterns.
 */
@Deprecated
@InterfaceAudience.Private
class KeyValueCompression {
  /**
   * Uncompresses a KeyValue from a DataInput and returns it.
   * 
   * @param in the DataInput
   * @param readContext the compressionContext to use.
   * @return an uncompressed KeyValue
   * @throws IOException
   */

  public static KeyValue readKV(DataInput in, CompressionContext readContext)
      throws IOException {
    int keylength = WritableUtils.readVInt(in);
    int vlength = WritableUtils.readVInt(in);
    int tagsLength = WritableUtils.readVInt(in);
    int length = (int) KeyValue.getKeyValueDataStructureSize(keylength, vlength, tagsLength);

    byte[] backingArray = new byte[length];
    int pos = 0;
    pos = Bytes.putInt(backingArray, pos, keylength);
    pos = Bytes.putInt(backingArray, pos, vlength);

    // the row
    int elemLen = Compressor.uncompressIntoArray(backingArray,
        pos + Bytes.SIZEOF_SHORT, in, readContext.rowDict);
    checkLength(elemLen, Short.MAX_VALUE);
    pos = Bytes.putShort(backingArray, pos, (short)elemLen);
    pos += elemLen;

    // family
    elemLen = Compressor.uncompressIntoArray(backingArray,
        pos + Bytes.SIZEOF_BYTE, in, readContext.familyDict);
    checkLength(elemLen, Byte.MAX_VALUE);
    pos = Bytes.putByte(backingArray, pos, (byte)elemLen);
    pos += elemLen;

    // qualifier
    elemLen = Compressor.uncompressIntoArray(backingArray, pos, in,
        readContext.qualifierDict);
    pos += elemLen;

    // the rest
    in.readFully(backingArray, pos, length - pos);

    return new KeyValue(backingArray, 0, length);
  }

  private static void checkLength(int len, int max) throws IOException {
    if (len < 0 || len > max) {
      throw new IOException(
          "Invalid length for compresesed portion of keyvalue: " + len);
    }
  }

  /**
   * Compresses and writes ourKV to out, a DataOutput.
   * 
   * @param out the DataOutput
   * @param keyVal the KV to compress and write
   * @param writeContext the compressionContext to use.
   * @throws IOException
   */
  public static void writeKV(final DataOutput out, KeyValue keyVal,
      CompressionContext writeContext) throws IOException {
    byte[] backingArray = keyVal.getBuffer();
    int offset = keyVal.getOffset();

    // we first write the KeyValue infrastructure as VInts.
    WritableUtils.writeVInt(out, keyVal.getKeyLength());
    WritableUtils.writeVInt(out, keyVal.getValueLength());
    WritableUtils.writeVInt(out, keyVal.getTagsLengthUnsigned());

    // now we write the row key, as the row key is likely to be repeated
    // We save space only if we attempt to compress elements with duplicates
    Compressor.writeCompressed(keyVal.getBuffer(), keyVal.getRowOffset(),
        keyVal.getRowLength(), out, writeContext.rowDict);

  
    // now family, if it exists. if it doesn't, we write a 0 length array.
    Compressor.writeCompressed(keyVal.getBuffer(), keyVal.getFamilyOffset(),
        keyVal.getFamilyLength(), out, writeContext.familyDict);

    // qualifier next
    Compressor.writeCompressed(keyVal.getBuffer(), keyVal.getQualifierOffset(),
        keyVal.getQualifierLength(), out,
        writeContext.qualifierDict);

    // now we write the rest uncompressed
    int pos = keyVal.getTimestampOffset();
    int remainingLength = keyVal.getLength() + offset - (pos);
    out.write(backingArray, pos, remainingLength);
  }
}
