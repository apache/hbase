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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link HFile} writer for version 3.
 */
@InterfaceAudience.Private
public class HFileWriterV3 extends HFileWriterV2 {

  private static final Log LOG = LogFactory.getLog(HFileWriterV3.class);

  private int maxTagsLength = 0;

  static class WriterFactoryV3 extends HFile.WriterFactory {
    WriterFactoryV3(Configuration conf, CacheConfig cacheConf) {
      super(conf, cacheConf);
    }

    @Override
    public Writer createWriter(FileSystem fs, Path path, FSDataOutputStream ostream,
        final KVComparator comparator, HFileContext fileContext)
        throws IOException {
      return new HFileWriterV3(conf, cacheConf, fs, path, ostream, comparator, fileContext);
    }
  }

  /** Constructor that takes a path, creates and closes the output stream. */
  public HFileWriterV3(Configuration conf, CacheConfig cacheConf, FileSystem fs, Path path,
      FSDataOutputStream ostream, final KVComparator comparator,
      final HFileContext fileContext) throws IOException {
    super(conf, cacheConf, fs, path, ostream, comparator, fileContext);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writer" + (path != null ? " for " + path : "") +
        " initialized with cacheConf: " + cacheConf +
        " comparator: " + comparator.getClass().getSimpleName() +
        " fileContext: " + fileContext);
    }
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the
   * Comparator passed on construction.
   * 
   * @param cell
   *          Cell to add. Cannot be empty nor null.
   * @throws IOException
   */
  @Override
  public void append(final Cell cell) throws IOException {
    // Currently get the complete arrays
    super.append(cell);
    int tagsLength = cell.getTagsLength();
    if (tagsLength > this.maxTagsLength) {
      this.maxTagsLength = tagsLength;
    }
  }

  protected void finishFileInfo() throws IOException {
    super.finishFileInfo();
    if (hFileContext.getDataBlockEncoding() == DataBlockEncoding.PREFIX_TREE) {
      // In case of Prefix Tree encoding, we always write tags information into HFiles even if all
      // KVs are having no tags.
      fileInfo.append(FileInfo.MAX_TAGS_LEN, Bytes.toBytes(this.maxTagsLength), false);
    } else if (hFileContext.isIncludesTags()) {
      // When tags are not being written in this file, MAX_TAGS_LEN is excluded
      // from the FileInfo
      fileInfo.append(FileInfo.MAX_TAGS_LEN, Bytes.toBytes(this.maxTagsLength), false);
      boolean tagsCompressed = (hFileContext.getDataBlockEncoding() != DataBlockEncoding.NONE)
        && hFileContext.isCompressTags();
      fileInfo.append(FileInfo.TAGS_COMPRESSED, Bytes.toBytes(tagsCompressed), false);
    }
  }

  @Override
  protected int getMajorVersion() {
    return 3;
  }

  @Override
  protected int getMinorVersion() {
    return HFileReaderV3.MAX_MINOR_VERSION;
  }

  @Override
  protected void finishClose(FixedFileTrailer trailer) throws IOException {
    // Write out encryption metadata before finalizing if we have a valid crypto context
    Encryption.Context cryptoContext = hFileContext.getEncryptionContext();
    if (cryptoContext != Encryption.Context.NONE) {
      // Wrap the context's key and write it as the encryption metadata, the wrapper includes
      // all information needed for decryption
      trailer.setEncryptionKey(EncryptionUtil.wrapKey(cryptoContext.getConf(),
        cryptoContext.getConf().get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
          User.getCurrent().getShortName()),
        cryptoContext.getKey()));
    }
    // Now we can finish the close
    super.finishClose(trailer);
  }

}
