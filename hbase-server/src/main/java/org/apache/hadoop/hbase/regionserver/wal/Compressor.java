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
 * limitations under the License
 */

package org.apache.hadoop.hbase.regionserver.wal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.Dictionary;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

/**
 * A set of static functions for running our custom WAL compression/decompression.
 * Also contains a command line tool to compress and uncompress HLogs.
 */
@InterfaceAudience.Private
public class Compressor {
  /**
   * Command line tool to compress and uncompress WALs.
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 2 || args[0].equals("--help") || args[0].equals("-h")) {
      printHelp();
      System.exit(-1);
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    transformFile(inputPath, outputPath);
  }

  private static void printHelp() {
    System.err.println("usage: Compressor <input> <output>");
    System.err.println("If <input> HLog is compressed, <output> will be decompressed.");
    System.err.println("If <input> HLog is uncompressed, <output> will be compressed.");
    return;
  }

  private static void transformFile(Path input, Path output)
      throws IOException {
    Configuration conf = HBaseConfiguration.create();

    FileSystem inFS = input.getFileSystem(conf);
    FileSystem outFS = output.getFileSystem(conf);

    HLog.Reader in = HLogFactory.createReader(inFS, input, conf, null, false);
    HLog.Writer out = null;

    try {
      if (!(in instanceof ReaderBase)) {
        System.err.println("Cannot proceed, invalid reader type: " + in.getClass().getName());
        return;
      }
      boolean compress = ((ReaderBase)in).hasCompression();
      conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, !compress);
      out = HLogFactory.createWALWriter(outFS, output, conf);

      HLog.Entry e = null;
      while ((e = in.next()) != null) out.append(e);
    } finally {
      in.close();
      if (out != null) {
        out.close();
        out = null;
      }
    }
  }

  /**
   * Reads the next compressed entry and returns it as a byte array
   * 
   * @param in the DataInput to read from
   * @param dict the dictionary we use for our read.
   * @return the uncompressed array.
   */
  @Deprecated
  static byte[] readCompressed(DataInput in, Dictionary dict)
      throws IOException {
    byte status = in.readByte();

    if (status == Dictionary.NOT_IN_DICTIONARY) {
      int length = WritableUtils.readVInt(in);
      // if this isn't in the dictionary, we need to add to the dictionary.
      byte[] arr = new byte[length];
      in.readFully(arr);
      if (dict != null) dict.addEntry(arr, 0, length);
      return arr;
    } else {
      // Status here is the higher-order byte of index of the dictionary entry
      // (when its not Dictionary.NOT_IN_DICTIONARY -- dictionary indices are
      // shorts).
      short dictIdx = toShort(status, in.readByte());
      byte[] entry = dict.getEntry(dictIdx);
      if (entry == null) {
        throw new IOException("Missing dictionary entry for index "
            + dictIdx);
      }
      return entry;
    }
  }

  /**
   * Reads a compressed entry into an array.
   * The output into the array ends up length-prefixed.
   * 
   * @param to the array to write into
   * @param offset array offset to start writing to
   * @param in the DataInput to read from
   * @param dict the dictionary to use for compression
   * 
   * @return the length of the uncompressed data
   */
  @Deprecated
  static int uncompressIntoArray(byte[] to, int offset, DataInput in,
      Dictionary dict) throws IOException {
    byte status = in.readByte();

    if (status == Dictionary.NOT_IN_DICTIONARY) {
      // status byte indicating that data to be read is not in dictionary.
      // if this isn't in the dictionary, we need to add to the dictionary.
      int length = WritableUtils.readVInt(in);
      in.readFully(to, offset, length);
      dict.addEntry(to, offset, length);
      return length;
    } else {
      // the status byte also acts as the higher order byte of the dictionary
      // entry
      short dictIdx = toShort(status, in.readByte());
      byte[] entry;
      try {
        entry = dict.getEntry(dictIdx);
      } catch (Exception ex) {
        throw new IOException("Unable to uncompress the log entry", ex);
      }
      if (entry == null) {
        throw new IOException("Missing dictionary entry for index "
            + dictIdx);
      }
      // now we write the uncompressed value.
      Bytes.putBytes(to, offset, entry, 0, entry.length);
      return entry.length;
    }
  }

  /**
   * Compresses and writes an array to a DataOutput
   * 
   * @param data the array to write.
   * @param out the DataOutput to write into
   * @param dict the dictionary to use for compression
   */
  @Deprecated
  static void writeCompressed(byte[] data, int offset, int length,
      DataOutput out, Dictionary dict)
      throws IOException {
    short dictIdx = Dictionary.NOT_IN_DICTIONARY;
    if (dict != null) {
      dictIdx = dict.findEntry(data, offset, length);
    }
    if (dictIdx == Dictionary.NOT_IN_DICTIONARY) {
      // not in dict
      out.writeByte(Dictionary.NOT_IN_DICTIONARY);
      WritableUtils.writeVInt(out, length);
      out.write(data, offset, length);
    } else {
      out.writeShort(dictIdx);
    }
  }

  static short toShort(byte hi, byte lo) {
    short s = (short) (((hi & 0xFF) << 8) | (lo & 0xFF));
    Preconditions.checkArgument(s >= 0);
    return s;
  }
}
