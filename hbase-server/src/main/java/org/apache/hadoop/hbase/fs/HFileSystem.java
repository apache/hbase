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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An encapsulation for the FileSystem object that hbase uses to access
 * data. This class allows the flexibility of using  
 * separate filesystem objects for reading and writing hfiles and hlogs.
 * In future, if we want to make hlogs be in a different filesystem,
 * this is the place to make it happen.
 */
public class HFileSystem extends FilterFileSystem {
  public static final Log LOG = LogFactory.getLog(HFileSystem.class);

  private final FileSystem noChecksumFs;   // read hfile data from storage
  private final boolean useHBaseChecksum;

  /**
   * Create a FileSystem object for HBase regionservers.
   * @param conf The configuration to be used for the filesystem
   * @param useHBaseChecksum if true, then use
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

    // disable checksum verification for local fileSystem, see HBASE-11218
    if (fs instanceof LocalFileSystem) {
      setWriteChecksum(fs, false);
      setVerifyChecksum(fs, false);
    }

    addLocationsOrderInterceptor(conf);

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

  private static void setWriteChecksum(FileSystem fs, boolean writeChecksum) throws IOException {
    // Some Hadoop versions will not have this method available
    try {
      FileSystem.class.getMethod("setWriteChecksum", boolean.class).invoke(fs, writeChecksum);
    } catch (NoSuchMethodException e) {
      LOG.debug("FileSystem does not support setWriteChecksum, ignoring");
    } catch (Throwable t) {
      throw new IOException("setWriteChecksum failed", t);
    }
  }

  private static void setVerifyChecksum(FileSystem fs, boolean verifyChecksum) throws IOException {
    fs.setVerifyChecksum(verifyChecksum);
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

  public static boolean addLocationsOrderInterceptor(Configuration conf) throws IOException {
    return addLocationsOrderInterceptor(conf, new ReorderWALBlocks());
  }

  /**
   * Add an interceptor on the calls to the namenode#getBlockLocations from the DFSClient
   * linked to this FileSystem. See HBASE-6435 for the background.
   * <p/>
   * There should be no reason, except testing, to create a specific ReorderBlocks.
   *
   * @return true if the interceptor was added, false otherwise.
   */
  static boolean addLocationsOrderInterceptor(Configuration conf, final ReorderBlocks lrb) {
    if (!conf.getBoolean("hbase.filesystem.reorder.blocks", true)) {  // activated by default
      LOG.debug("addLocationsOrderInterceptor configured to false");
      return false;
    }

    FileSystem fs;
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.warn("Can't get the file system from the conf.", e);
      return false;
    }

    if (!(fs instanceof DistributedFileSystem)) {
      LOG.debug("The file system is not a DistributedFileSystem. " +
          "Skipping on block location reordering");
      return false;
    }

    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    DFSClient dfsc = dfs.getClient();
    if (dfsc == null) {
      LOG.warn("The DistributedFileSystem does not contain a DFSClient. Can't add the location " +
          "block reordering interceptor. Continuing, but this is unexpected."
      );
      return false;
    }

    try {
      Field nf = DFSClient.class.getDeclaredField("namenode");
      nf.setAccessible(true);
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(nf, nf.getModifiers() & ~Modifier.FINAL);

      ClientProtocol namenode = (ClientProtocol) nf.get(dfsc);
      if (namenode == null) {
        LOG.warn("The DFSClient is not linked to a namenode. Can't add the location block" +
            " reordering interceptor. Continuing, but this is unexpected."
        );
        return false;
      }

      ClientProtocol cp1 = createReorderingProxy(namenode, lrb, conf);
      nf.set(dfsc, cp1);
      LOG.info("Added intercepting call to namenode#getBlockLocations so can do block reordering" +
        " using class " + lrb.getClass());
    } catch (NoSuchFieldException e) {
      LOG.warn("Can't modify the DFSClient#namenode field to add the location reorder.", e);
      return false;
    } catch (IllegalAccessException e) {
      LOG.warn("Can't modify the DFSClient#namenode field to add the location reorder.", e);
      return false;
    }

    return true;
  }

  private static ClientProtocol createReorderingProxy(final ClientProtocol cp,
      final ReorderBlocks lrb, final Configuration conf) {
    return (ClientProtocol) Proxy.newProxyInstance
        (cp.getClass().getClassLoader(),
            new Class[]{ClientProtocol.class, Closeable.class},
            new InvocationHandler() {
              public Object invoke(Object proxy, Method method,
                                   Object[] args) throws Throwable {
                try {
                  if ((args == null || args.length == 0)
                      && "close".equals(method.getName())) {
                    RPC.stopProxy(cp);
                    return null;
                  } else {
                    Object res = method.invoke(cp, args);
                    if (res != null && args != null && args.length == 3
                        && "getBlockLocations".equals(method.getName())
                        && res instanceof LocatedBlocks
                        && args[0] instanceof String
                        && args[0] != null) {
                      lrb.reorderBlocks(conf, (LocatedBlocks) res, (String) args[0]);
                    }
                    return res;
                  }
                } catch  (InvocationTargetException ite) {
                  // We will have this for all the exception, checked on not, sent
                  //  by any layer, including the functional exception
                  Throwable cause = ite.getCause();
                  if (cause == null){
                    throw new RuntimeException(
                      "Proxy invocation failed and getCause is null", ite);
                  }
                  if (cause instanceof UndeclaredThrowableException) {
                    Throwable causeCause = cause.getCause();
                    if (causeCause == null) {
                      throw new RuntimeException("UndeclaredThrowableException had null cause!");
                    }
                    cause = cause.getCause();
                  }
                  throw cause;
                }
              }
            });
  }

  /**
   * Interface to implement to add a specific reordering logic in hdfs.
   */
  interface ReorderBlocks {
    /**
     *
     * @param conf - the conf to use
     * @param lbs - the LocatedBlocks to reorder
     * @param src - the file name currently read
     * @throws IOException - if something went wrong
     */
    void reorderBlocks(Configuration conf, LocatedBlocks lbs, String src) throws IOException;
  }

  /**
   * We're putting at lowest priority the hlog files blocks that are on the same datanode
   * as the original regionserver which created these files. This because we fear that the
   * datanode is actually dead, so if we use it it will timeout.
   */
  static class ReorderWALBlocks implements ReorderBlocks {
    public void reorderBlocks(Configuration conf, LocatedBlocks lbs, String src)
        throws IOException {

      ServerName sn = HLogUtil.getServerNameFromHLogDirectoryName(conf, src);
      if (sn == null) {
        // It's not an HLOG
        return;
      }

      // Ok, so it's an HLog
      String hostName = sn.getHostname();
      if (LOG.isTraceEnabled()) {
        LOG.trace(src +
            " is an HLog file, so reordering blocks, last hostname will be:" + hostName);
      }

      // Just check for all blocks
      for (LocatedBlock lb : lbs.getLocatedBlocks()) {
        DatanodeInfo[] dnis = lb.getLocations();
        if (dnis != null && dnis.length > 1) {
          boolean found = false;
          for (int i = 0; i < dnis.length - 1 && !found; i++) {
            if (hostName.equals(dnis[i].getHostName())) {
              // advance the other locations by one and put this one at the last place.
              DatanodeInfo toLast = dnis[i];
              System.arraycopy(dnis, i + 1, dnis, i, dnis.length - i - 1);
              dnis[dnis.length - 1] = toLast;
              found = true;
            }
          }
        }
      }
    }
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
  @SuppressWarnings("deprecation")
  public FSDataOutputStream createNonRecursive(Path f,
      boolean overwrite,
      int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return fs.createNonRecursive(f, overwrite, bufferSize, replication,
                                 blockSize, progress);
  }
}
