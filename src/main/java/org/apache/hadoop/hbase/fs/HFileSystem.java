/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.fs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Progressable;

/**
 * An encapsulation for the FileSystem object that hbase uses to access
 * data. This class allows the flexibility of using  
 * separate filesystem objects for reading and writing hfiles and hlogs.
 * In future, if we want to make hlogs be in a different filesystem,
 * this is the place to make it happen.
 */
public class HFileSystem extends FilterFileSystem {

  private final FileSystem noChecksumFs;   // read hfile data from storage
  private final boolean useHBaseChecksum;

  /**
   * Create a FileSystem object for HBase regionservers.
   * @param conf The configuration to be used for the filesystem
   * @param useHBaseChecksums if true, then use
   *        checksum verfication in hbase, otherwise
   *        delegate checksum verification to the FileSystem.
   */
  public HFileSystem(Configuration conf, boolean useHBaseChecksum)
    throws IOException {

    // Create the default filesystem with checksum verification switched on.
    // By default, any operation to this FilterFileSystem occurs on
    // the underlying filesystem that has checksums switched on.
    this.fs = FileSystem.get(conf);
    this.useHBaseChecksum = useHBaseChecksum;
    
    fs.initialize(getDefaultUri(conf), conf);

    // If hbase checksum verification is switched on, then create a new
    // filesystem object that has cksum verification turned off.
    // We will avoid verifying checksums in the fs client, instead do it
    // inside of hbase.
    // If this is the local file system hadoop has a bug where seeks
    // do not go to the correct location if setVerifyChecksum(false) is called.
    // This manifests itself in that incorrect data is read and HFileBlocks won't be able to read
    // their header magic numbers. See HBASE-5885
    if (useHBaseChecksum && !(fs instanceof LocalFileSystem)) {
      conf = new Configuration(conf);
      conf.setBoolean("dfs.client.read.shortcircuit.skip.checksum", true);
      this.noChecksumFs = newInstanceFileSystem(conf);
      this.noChecksumFs.setVerifyChecksum(false);
    } else {
      this.noChecksumFs = fs;
    }
  }

  /**
   * Wrap a FileSystem object within a HFileSystem. The noChecksumFs and
   * writefs are both set to be the same specified fs. 
   * Do not verify hbase-checksums while reading data from filesystem.
   * @param fs Set the noChecksumFs and writeFs to this specified filesystem.
   */
  public HFileSystem(FileSystem fs) {
    this.fs = fs;
    this.noChecksumFs = fs;
    this.useHBaseChecksum = false;
  }

  /**
   * Returns the filesystem that is specially setup for 
   * doing reads from storage. This object avoids doing 
   * checksum verifications for reads.
   * @return The FileSystem object that can be used to read data
   *         from files.
   */
  public FileSystem getNoChecksumFs() {
    return noChecksumFs;
  }

  /**
   * Returns the underlying filesystem
   * @return The underlying FileSystem for this FilterFileSystem object.
   */
  public FileSystem getBackingFs() throws IOException {
    return fs;
  }

  /**
   * Are we verifying checksums in HBase?
   * @return True, if hbase is configured to verify checksums,
   *         otherwise false.
   */
  public boolean useHBaseChecksum() {
    return useHBaseChecksum;
  }

  /**
   * Close this filesystem object
   */
  @Override
  public void close() throws IOException {
    super.close();
    if (this.noChecksumFs != fs) {
      this.noChecksumFs.close();
    }
  }

 /**
   * Returns a brand new instance of the FileSystem. It does not use
   * the FileSystem.Cache. In newer versions of HDFS, we can directly
   * invoke FileSystem.newInstance(Configuration).
   * 
   * @param conf Configuration
   * @return A new instance of the filesystem
   */
  private static FileSystem newInstanceFileSystem(Configuration conf)
    throws IOException {
    URI uri = FileSystem.getDefaultUri(conf);
    FileSystem fs = null;
    Class<?> clazz = conf.getClass("fs." + uri.getScheme() + ".impl", null);
    if (clazz != null) {
      // This will be true for Hadoop 1.0, or 0.20.
      fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
      fs.initialize(uri, conf);
    } else {
      // For Hadoop 2.0, we have to go through FileSystem for the filesystem
      // implementation to be loaded by the service loader in case it has not
      // been loaded yet.
      Configuration clone = new Configuration(conf);
      clone.setBoolean("fs." + uri.getScheme() + ".impl.disable.cache", true);
      fs = FileSystem.get(uri, clone);
    }
    if (fs == null) {
      throw new IOException("No FileSystem for scheme: " + uri.getScheme());
    }
    return fs;
  }

  /**
   * Create a new HFileSystem object, similar to FileSystem.get().
   * This returns a filesystem object that avoids checksum
   * verification in the filesystem for hfileblock-reads.
   * For these blocks, checksum verification is done by HBase.
   */
  static public FileSystem get(Configuration conf) throws IOException {
    return new HFileSystem(conf, true);
  }

  /**
   * Wrap a LocalFileSystem within a HFileSystem.
   */
  static public FileSystem getLocalFs(Configuration conf) throws IOException {
    return new HFileSystem(FileSystem.getLocal(conf));
  }

  /**
   * The org.apache.hadoop.fs.FilterFileSystem does not yet support 
   * createNonRecursive. This is a hadoop bug and when it is fixed in Hadoop,
   * this definition will go away.
   */
  public FSDataOutputStream createNonRecursive(Path f,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return fs.createNonRecursive(f, overwrite, bufferSize, replication,
                                 blockSize, progress);
  }
}
