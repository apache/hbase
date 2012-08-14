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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mockito.Mockito;

/**
 * Compact passed set of files.
 * Create an instance and then call {@ink #compact(Store, Collection, boolean, long)}.
 * Call this classes {@link #main(String[])} to see how to run compaction code
 * 'standalone'.
 */
public class CompactionTool implements Tool {
  // Instantiates a Store instance and a mocked up HRegion.  The compaction code
  // requires a StoreScanner and a StoreScanner has to have a Store; its too
  // tangled to do without (Store needs an HRegion which is another tangle).
  // TODO: Undo the tangles some day.
  private Configuration conf;

  CompactionTool() {
    super();
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration c) {
    this.conf = c;
  }

  private int usage(final int errCode) {
    return usage(errCode, null);
  }

  private int usage(final int errCode, final String errMsg) {
    if (errMsg != null) System.err.println("ERROR: " + errMsg);
    System.err.println("Usage: CompactionTool [options] <inputdir>");
    System.err.println(" To preserve input files, pass -Dhbase.hstore.compaction.complete=false");
    System.err.println(" To set tmp dir, pass -Dhbase.tmp.dir=ALTERNATE_DIR");
    System.err.println(" To stop delete of compacted file, pass -Dhbase.compactiontool.delete=false");
    return errCode;
  }

  int checkdir(final FileSystem fs, final Path p) throws IOException {
    if (!fs.exists(p)) {
      return usage(-2, p.toString() + " does not exist.");
    }
    if (!fs.getFileStatus(p).isDir()) {
      return usage(-3, p.toString() + " must be a directory");
    }
    return 0;
  }

  /**
   * Mock up an HRegion instance.  Need to return an HRegionInfo when asked.
   * Also need an executor to run storefile open/closes.  Need to repeat
   * the thenReturn on getOpenAndCloseThreadPool because otherwise it returns
   * cache of first which is closed during the opening.  Also, need to return
   * tmpdir, etc.
   * @param hri
   * @param tmpdir
   * @return
   */
  private HRegion createHRegion(final HRegionInfo hri, final Path tmpdir) {
    HRegion mockedHRegion = Mockito.mock(HRegion.class);
    Mockito.when(mockedHRegion.getRegionInfo()).thenReturn(hri);
    Mockito.when(mockedHRegion.getStoreFileOpenAndCloseThreadPool(Mockito.anyString())).
      thenReturn(HRegion.getOpenAndCloseThreadPool(1, "mockedRegion.opener")).
      thenReturn(HRegion.getOpenAndCloseThreadPool(1, "mockedRegion.closer"));
    Mockito.when(mockedHRegion.areWritesEnabled()).thenReturn(true);
    Mockito.when(mockedHRegion.getTmpDir()).thenReturn(tmpdir);
    return mockedHRegion;
  }

  /**
   * Fake up a Store around the passed <code>storedir</code>.
   * @param fs
   * @param storedir
   * @param tmpdir
   * @return
   * @throws IOException
   */
  private Store getStore(final FileSystem fs, final Path storedir, final Path tmpdir)
  throws IOException {
    // TODO: Let config on table and column family be configurable from
    // command-line setting versions, etc.  For now do defaults
    HColumnDescriptor hcd = new HColumnDescriptor("f");
    HRegionInfo hri = new HRegionInfo(Bytes.toBytes("t"));
    // Get a shell of an HRegion w/ enough functionality to make Store happy.
    HRegion region = createHRegion(hri, tmpdir);
    // Create a Store w/ check of hbase.rootdir blanked out and return our
    // list of files instead of have Store search its home dir.
    return new Store(tmpdir, region, hcd, fs, getConf()) {
      @Override
      public FileStatus[] getStoreFiles() throws IOException {
        return this.fs.listStatus(getHomedir());
      }

      @Override
      Path createStoreHomeDir(FileSystem fs, Path homedir) throws IOException {
        return storedir;
      }
    };
  }

  @Override
  public int run(final String[] args) throws Exception {
    if (args.length == 0) return usage(-1);
    FileSystem fs = FileSystem.get(getConf());
    final Path inputdir = new Path(args[0]);
    final Path tmpdir = new Path(getConf().get("hbase.tmp.dir"));
    int errCode = checkdir(fs, inputdir);
    if (errCode != 0) return errCode;
    errCode = checkdir(fs, tmpdir);
    if (errCode != 0) return errCode;
    // Get a Store that wraps the inputdir of files to compact.
    Store store = getStore(fs, inputdir, tmpdir);
    // Now we have a Store, run a compaction of passed files.
    try {
      CompactionRequest cr = store.requestCompaction();
      StoreFile sf = store.compact(cr);
      if (sf != null) {
        sf.closeReader(true);
        if (conf.getBoolean("hbase.compactiontool.delete", true)) {
          if (!fs.delete(sf.getPath(), false)) {
            throw new IOException("Failed delete of " + sf.getPath());
          }
        }
      }
    } finally {
      store.close();
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(),
      new CompactionTool(), args));
  }
}
