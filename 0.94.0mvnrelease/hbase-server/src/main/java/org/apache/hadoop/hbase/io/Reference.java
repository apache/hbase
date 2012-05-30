/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.ByteString;

/**
 * A reference to the top or bottom half of a store file where 'bottom' is the first half
 * of the file containing the keys that sort lowest and 'top' is the second half
 * of the file with keys that sort greater than those of the bottom half.  The file referenced
 * lives under a different region.  References are made at region split time.
 *
 * <p>References work with a special half store file type.  References know how
 * to write out the reference format in the file system and are what is juggled
 * when references are mixed in with direct store files.  The half store file
 * type is used reading the referred to file.
 *
 * <p>References to store files located over in some other region look like
 * this in the file system
 * <code>1278437856009925445.3323223323</code>:
 * i.e. an id followed by hash of the referenced region.
 * Note, a region is itself not splittable if it has instances of store file
 * references.  References are cleaned up by compactions.
 */
@InterfaceAudience.Private
public class Reference implements Writable {
  private byte [] splitkey;
  private Range region;

  /**
   * For split HStoreFiles, it specifies if the file covers the lower half or
   * the upper half of the key range
   */
  static enum Range {
    /** HStoreFile contains upper half of key range */
    top,
    /** HStoreFile contains lower half of key range */
    bottom
  }

  /**
   * @param splitRow
   * @return A {@link Reference} that points at top half of a an hfile
   */
  public static Reference createTopReference(final byte [] splitRow) {
    return new Reference(splitRow, Range.top);
  }

  /**
   * @param splitRow
   * @return A {@link Reference} that points at the bottom half of a an hfile
   */
  public static Reference createBottomReference(final byte [] splitRow) {
    return new Reference(splitRow, Range.bottom);
  }

  /**
   * Constructor
   * @param splitRow This is row we are splitting around.
   * @param fr
   */
  Reference(final byte [] splitRow, final Range fr) {
    this.splitkey = splitRow == null?  null: KeyValue.createFirstOnRow(splitRow).getKey();
    this.region = fr;
  }

  /**
   * Used by serializations.
   * @deprecated Use the pb serializations instead.  Writables are going away.
   */
  @Deprecated
  // Make this private when it comes time to let go of this constructor.  Needed by pb serialization.
  public Reference() {
    this(null, Range.bottom);
  }

  /**
   *
   * @return Range
   */
  public Range getFileRegion() {
    return this.region;
  }

  /**
   * @return splitKey
   */
  public byte [] getSplitKey() {
    return splitkey;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "" + this.region;
  }

  /**
   * @deprecated Writables are going away. Use the pb serialization methods instead.
   */
  @Deprecated
  public void write(DataOutput out) throws IOException {
    // Write true if we're doing top of the file.
    out.writeBoolean(isTopFileRegion(this.region));
    Bytes.writeByteArray(out, this.splitkey);
  }

  /**
   * @deprecated Writables are going away. Use the pb serialization methods instead.
   */
  @Deprecated
  public void readFields(DataInput in) throws IOException {
    boolean tmp = in.readBoolean();
    // If true, set region to top.
    this.region = tmp? Range.top: Range.bottom;
    this.splitkey = Bytes.readByteArray(in);
  }

  public static boolean isTopFileRegion(final Range r) {
    return r.equals(Range.top);
  }

  public Path write(final FileSystem fs, final Path p)
  throws IOException {
    FSDataOutputStream out = fs.create(p, false);
    try {
      out.write(toByteArray());
    } finally {
      out.close();
    }
    return p;
  }

  /**
   * Read a Reference from FileSystem.
   * @param fs
   * @param p
   * @return New Reference made from passed <code>p</code>
   * @throws IOException
   */
  public static Reference read(final FileSystem fs, final Path p)
  throws IOException {
    FSDataInputStream in = fs.open(p);
    try {
      return parseFrom(in);
    } finally {
      in.close();
    }
  }

  FSProtos.Reference convert() {
    FSProtos.Reference.Builder builder = FSProtos.Reference.newBuilder();
    builder.setRange(isTopFileRegion(getFileRegion())?
      FSProtos.Reference.Range.TOP: FSProtos.Reference.Range.BOTTOM);
    builder.setSplitkey(ByteString.copyFrom(getSplitKey()));
    return builder.build();
  }

  static Reference convert(final FSProtos.Reference r) {
    Reference result = new Reference();
    result.splitkey = r.getSplitkey().toByteArray();
    result.region = r.getRange() == FSProtos.Reference.Range.TOP? Range.top: Range.bottom;
    return result;
  }

  /**
   * Use this instead of {@link #toByteArray()} when writing to a stream and you want to use
   * the pb mergeDelimitedFrom (w/o the delimiter, pb reads to EOF which may not be what ou want).
   * @return This instance serialized as a delimited protobuf w/ a magic pb prefix.
   * @throws IOException
   * @see {@link #toByteArray()}
   */
  byte [] toByteArray() throws IOException {
    return ProtobufUtil.prependPBMagic(convert().toByteArray());
  }

  /**
   * Parses an {@link Reference} instance from the passed in stream.  Presumes the
   * Reference was serialized to the stream with {@link #toDelimitedByteArray()}
   * @param in
   * @return An instance of Reference.
   * @throws IOException
   */
  static Reference parseFrom(final FSDataInputStream in) throws IOException {
    // I need to be able to move back in the stream if this is not a pb serialization so I can
    // do the Writable decoding instead.
    InputStream is = in.markSupported()? in: new BufferedInputStream(in);
    int pblen = ProtobufUtil.lengthOfPBMagic();
    is.mark(pblen);
    byte [] pbuf = new byte[pblen];
    int read = is.read(pbuf);
    if (read != pblen) throw new IOException("read=" + read + ", wanted=" + pblen);
    if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
      return convert(FSProtos.Reference.parseFrom(is));
    } else {
      // Presume Writables.  Need to reset the stream since it didn't start w/ pb.
      // We won't bother rewriting thie Reference as a pb since Reference is transitory.
      in.reset();
      Reference r = new Reference();
      r.readFields(in);
      return r;
    }
  }
}