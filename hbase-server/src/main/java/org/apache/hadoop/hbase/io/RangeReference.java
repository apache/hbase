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
package org.apache.hadoop.hbase.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;

//TODO: change description of range reference
/**
 * A reference to the top or bottom half of a store file where 'bottom' is the first half of the
 * file containing the keys that sort lowest and 'top' is the second half of the file with keys that
 * sort greater than those of the bottom half. The file referenced lives under a different region.
 * References are made at region split time.
 * <p>
 * References work with a special half store file type. References know how to write out the
 * reference format in the file system and are what is juggled when references are mixed in with
 * direct store files. The half store file type is used reading the referred to file.
 * <p>
 * References to store files located over in some other region look like this in the file system
 * <code>1278437856009925445.3323223323</code>: i.e. an id followed by hash of the referenced
 * region. Note, a region is itself not splittable if it has instances of store file references.
 * References are cleaned up by compactions.
 */
@InterfaceAudience.Private
public class RangeReference {
  private byte[] startKey;

  private byte[] endKey;

  public RangeReference() {

  }

  /** Returns A {@link RangeReference} that points at top half of a an hfile */
  public static RangeReference createRangeReference(final byte[] startKey, final byte[] endKey) {
    return new RangeReference(startKey, endKey);
  }

  /**
   * Constructor TODO: @param splitRow This is row we are splitting around.
   */
  RangeReference(final byte[] startKey, final byte[] endKey) {
    this.startKey = startKey == null ? null : KeyValueUtil.createFirstOnRow(startKey).getKey();
    this.endKey = endKey == null ? null : KeyValueUtil.createFirstOnRow(endKey).getKey();
  }

  /**
   *   */
  public byte[] getStartKey() {
    return startKey;
  }

  /**
   *   */
  public byte[] getEndKey() {
    return endKey;
  }

  public Path write(final FileSystem fs, final Path p) throws IOException {
    try (FSDataOutputStream out = fs.create(p, false)) {
      out.write(toByteArray());
    }
    return p;
  }

  /**
   * Read a RangeReference from FileSystem.
   * @return New RangeReference made from passed <code>p</code>
   */
  public static RangeReference read(final FileSystem fs, final Path p) throws IOException {
    InputStream in = fs.open(p);
    try {
      // I need to be able to move back in the stream if this is not a pb serialization so I can
      // do the Writable decoding instead.
      in = in.markSupported() ? in : new BufferedInputStream(in);
      int pblen = ProtobufUtil.lengthOfPBMagic();
      in.mark(pblen);
      byte[] pbuf = new byte[pblen];
      IOUtils.readFully(in, pbuf, 0, pblen);
      // WATCHOUT! Return in middle of function!!!
      if (ProtobufUtil.isPBMagicPrefix(pbuf)) return convert(FSProtos.RangeReference.parseFrom(in));
      throw new IOException("Unable to parse the Range Reference from the protobuf.");
    } finally {
      in.close();
    }
  }

  public FSProtos.RangeReference convert() {
    FSProtos.RangeReference.Builder builder = FSProtos.RangeReference.newBuilder();
    builder.setStartKey(UnsafeByteOperations.unsafeWrap(getStartKey()));
    builder.setEndKey(UnsafeByteOperations.unsafeWrap(getEndKey()));
    return builder.build();
  }

  public static RangeReference convert(final FSProtos.RangeReference r) {
    RangeReference result = new RangeReference();
    result.startKey = r.getStartKey().toByteArray();
    result.endKey = r.getEndKey().toByteArray();
    return result;
  }

  /**
   * Use this when writing to a stream and you want to use the pb mergeDelimitedFrom (w/o the
   * delimiter, pb reads to EOF which may not be what you want).
   * @return This instance serialized as a delimited protobuf w/ a magic pb prefix.
   */
  byte[] toByteArray() throws IOException {
    return ProtobufUtil.prependPBMagic(convert().toByteArray());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(startKey) + Arrays.hashCode(endKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof RangeReference)) return false;

    RangeReference r = (RangeReference) o;
    if (startKey != null && r.startKey == null) return false;
    if (startKey == null && r.startKey != null) return false;
    if (endKey != null && r.endKey == null) return false;
    if (endKey == null && r.endKey != null) return false;
    if (startKey != null && !Arrays.equals(startKey, r.startKey)) return false;
    if (endKey != null && !Arrays.equals(endKey, r.endKey)) return false;
    return true;
  }
}
