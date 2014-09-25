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
package org.apache.hadoop.hbase.util.hbck;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CorruptHFileException;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSUtils.FamilyDirFilter;
import org.apache.hadoop.hbase.util.FSUtils.HFileFilter;
import org.apache.hadoop.hbase.util.FSUtils.RegionDirFilter;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;

/**
 * This class marches through all of the region's hfiles and verifies that
 * they are all valid files. One just needs to instantiate the class, use
 * checkTables(List<Path>) and then retrieve the corrupted hfiles (and
 * quarantined files if in quarantining mode)
 *
 * The implementation currently parallelizes at the regionDir level.
 */
@InterfaceAudience.Private
public class HFileCorruptionChecker {
  private static final Log LOG = LogFactory.getLog(HFileCorruptionChecker.class);

  final Configuration conf;
  final FileSystem fs;
  final CacheConfig cacheConf;
  final ExecutorService executor;
  final Set<Path> corrupted = new ConcurrentSkipListSet<Path>();
  final Set<Path> failures = new ConcurrentSkipListSet<Path>();
  final Set<Path> quarantined = new ConcurrentSkipListSet<Path>();
  final Set<Path> missing = new ConcurrentSkipListSet<Path>();
  final boolean inQuarantineMode;
  final AtomicInteger hfilesChecked = new AtomicInteger();

  public HFileCorruptionChecker(Configuration conf, ExecutorService executor,
      boolean quarantine) throws IOException {
    this.conf = conf;
    this.fs = FileSystem.get(conf);
    this.cacheConf = new CacheConfig(conf);
    this.executor = executor;
    this.inQuarantineMode = quarantine;
  }

  /**
   * Checks a path to see if it is a valid hfile.
   *
   * @param p
   *          full Path to an HFile
   * @throws IOException
   *           This is a connectivity related exception
   */
  protected void checkHFile(Path p) throws IOException {
    HFile.Reader r = null;
    try {
      r = HFile.createReader(fs, p, cacheConf, conf);
    } catch (CorruptHFileException che) {
      LOG.warn("Found corrupt HFile " + p, che);
      corrupted.add(p);
      if (inQuarantineMode) {
        Path dest = createQuarantinePath(p);
        LOG.warn("Quarantining corrupt HFile " + p + " into " + dest);
        boolean success = fs.mkdirs(dest.getParent());
        success = success ? fs.rename(p, dest): false;
        if (!success) {
          failures.add(p);
        } else {
          quarantined.add(dest);
        }
      }
      return;
    } catch (FileNotFoundException fnfe) {
      LOG.warn("HFile " + p + " was missing.  Likely removed due to compaction/split?");
      missing.add(p);
    } finally {
      hfilesChecked.addAndGet(1);
      if (r != null) {
        r.close(true);
      }
    }
  }

  /**
   * Given a path, generates a new path to where we move a corrupted hfile (bad
   * trailer, no trailer).
   *
   * @param hFile
   *          Path to a corrupt hfile (assumes that it is HBASE_DIR/ table
   *          /region/cf/file)
   * @return path to where corrupted files are stored. This should be
   *         HBASE_DIR/.corrupt/table/region/cf/file.
   */
  Path createQuarantinePath(Path hFile) throws IOException {
    // extract the normal dirs structure
    Path cfDir = hFile.getParent();
    Path regionDir = cfDir.getParent();
    Path tableDir = regionDir.getParent();

    // build up the corrupted dirs strcture
    Path corruptBaseDir = new Path(FSUtils.getRootDir(conf), conf.get(
        "hbase.hfile.quarantine.dir", HConstants.CORRUPT_DIR_NAME));
    Path corruptTableDir = new Path(corruptBaseDir, tableDir.getName());
    Path corruptRegionDir = new Path(corruptTableDir, regionDir.getName());
    Path corruptFamilyDir = new Path(corruptRegionDir, cfDir.getName());
    Path corruptHfile = new Path(corruptFamilyDir, hFile.getName());
    return corruptHfile;
  }

  /**
   * Check all files in a column family dir.
   *
   * @param cfDir
   *          column family directory
   * @throws IOException
   */
  protected void checkColFamDir(Path cfDir) throws IOException {
    FileStatus[] hfs = null;
    try {
      hfs = fs.listStatus(cfDir, new HFileFilter(fs)); // use same filter as scanner.
    } catch (FileNotFoundException fnfe) {
      // Hadoop 0.23+ listStatus semantics throws an exception if the path does not exist.
      LOG.warn("Colfam Directory " + cfDir +
          " does not exist.  Likely due to concurrent split/compaction. Skipping.");
      missing.add(cfDir);
      return;
    }

    // Hadoop 1.0 listStatus does not throw an exception if the path does not exist.
    if (hfs.length == 0 && !fs.exists(cfDir)) {
      LOG.warn("Colfam Directory " + cfDir +
          " does not exist.  Likely due to concurrent split/compaction. Skipping.");
      missing.add(cfDir);
      return;
    }
    for (FileStatus hfFs : hfs) {
      Path hf = hfFs.getPath();
      checkHFile(hf);
    }
  }

  /**
   * Check all column families in a region dir.
   *
   * @param regionDir
   *          region directory
   * @throws IOException
   */
  protected void checkRegionDir(Path regionDir) throws IOException {
    FileStatus[] cfs = null;
    try {
      cfs = fs.listStatus(regionDir, new FamilyDirFilter(fs));
    } catch (FileNotFoundException fnfe) {
      // Hadoop 0.23+ listStatus semantics throws an exception if the path does not exist.
      LOG.warn("Region Directory " + regionDir +
          " does not exist.  Likely due to concurrent split/compaction. Skipping.");
      missing.add(regionDir);
      return;
    }

    // Hadoop 1.0 listStatus does not throw an exception if the path does not exist.
    if (cfs.length == 0 && !fs.exists(regionDir)) {
      LOG.warn("Region Directory " + regionDir +
          " does not exist.  Likely due to concurrent split/compaction. Skipping.");
      missing.add(regionDir);
      return;
    }

    for (FileStatus cfFs : cfs) {
      Path cfDir = cfFs.getPath();
      checkColFamDir(cfDir);
    }
  }

  /**
   * Check all the regiondirs in the specified tableDir
   *
   * @param tableDir
   *          path to a table
   * @throws IOException
   */
  void checkTableDir(Path tableDir) throws IOException {
    FileStatus[] rds = fs.listStatus(tableDir, new RegionDirFilter(fs));
    if (rds.length == 0 && !fs.exists(tableDir)) {
      // interestingly listStatus does not throw an exception if the path does not exist.
      LOG.warn("Table Directory " + tableDir +
          " does not exist.  Likely due to concurrent delete. Skipping.");
      missing.add(tableDir);
      return;
    }

    // Parallelize check at the region dir level
    List<RegionDirChecker> rdcs = new ArrayList<RegionDirChecker>();
    List<Future<Void>> rdFutures;

    for (FileStatus rdFs : rds) {
      Path rdDir = rdFs.getPath();
      RegionDirChecker work = new RegionDirChecker(rdDir);
      rdcs.add(work);
    }

    // Submit and wait for completion
    try {
      rdFutures = executor.invokeAll(rdcs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOG.warn("Region dirs checking interrupted!", ie);
      return;
    }

    for (int i = 0; i < rdFutures.size(); i++) {
      Future<Void> f = rdFutures.get(i);
      try {
        f.get();
      } catch (ExecutionException e) {
        LOG.warn("Failed to quaratine an HFile in regiondir "
            + rdcs.get(i).regionDir, e.getCause());
        // rethrow IOExceptions
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }

        // rethrow RuntimeExceptions
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        }

        // this should never happen
        LOG.error("Unexpected exception encountered", e);
        return; // bailing out.
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        LOG.warn("Region dirs check interrupted!", ie);
        // bailing out
        return;
      }
    }
  }

  /**
   * An individual work item for parallelized regiondir processing. This is
   * intentionally an inner class so it can use the shared error sets and fs.
   */
  private class RegionDirChecker implements Callable<Void> {
    final Path regionDir;

    RegionDirChecker(Path regionDir) {
      this.regionDir = regionDir;
    }

    @Override
    public Void call() throws IOException {
      checkRegionDir(regionDir);
      return null;
    }
  }

  /**
   * Check the specified table dirs for bad hfiles.
   */
  public void checkTables(Collection<Path> tables) throws IOException {
    for (Path t : tables) {
      checkTableDir(t);
    }
  }

  /**
   * @return the set of check failure file paths after checkTables is called.
   */
  public Collection<Path> getFailures() {
    return new HashSet<Path>(failures);
  }

  /**
   * @return the set of corrupted file paths after checkTables is called.
   */
  public Collection<Path> getCorrupted() {
    return new HashSet<Path>(corrupted);
  }

  /**
   * @return number of hfiles checked in the last HfileCorruptionChecker run
   */
  public int getHFilesChecked() {
    return hfilesChecked.get();
  }

  /**
   * @return the set of successfully quarantined paths after checkTables is called.
   */
  public Collection<Path> getQuarantined() {
    return new HashSet<Path>(quarantined);
  }

  /**
   * @return the set of paths that were missing.  Likely due to deletion/moves from
   *  compaction or flushes.
   */
  public Collection<Path> getMissing() {
    return new HashSet<Path>(missing);
  }

  /**
   * Print a human readable summary of hfile quarantining operations.
   * @param out
   */
  public void report(ErrorReporter out) {
    out.print("Checked " + hfilesChecked.get() + " hfile for corruption");
    out.print("  HFiles corrupted:                  " + corrupted.size());
    if (inQuarantineMode) {
      out.print("    HFiles successfully quarantined: " + quarantined.size());
      for (Path sq : quarantined) {
        out.print("      " + sq);
      }
      out.print("    HFiles failed quarantine:        " + failures.size());
      for (Path fq : failures) {
        out.print("      " + fq);
      }
    }
    out.print("    HFiles moved while checking:     " + missing.size());
    for (Path mq : missing) {
      out.print("      " + mq);
    }

    String initialState = (corrupted.size() == 0) ? "OK" : "CORRUPTED";
    String fixedState = (corrupted.size() == quarantined.size()) ? "OK"
        : "CORRUPTED";

    if (inQuarantineMode) {
      out.print("Summary: " + initialState + " => " + fixedState);
    } else {
      out.print("Summary: " + initialState);
    }
  }
}
