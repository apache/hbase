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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool to detect presence of any HFileV1 in the given directory. It prints all such regions which
 * have such files.
 * <p>
 * To print the help section of the tool:
 * <ul>
 * <li>./bin/hbase org.apache.hadoop.hbase.util.HFileV1Detector --h or,
 * <li>java -cp `hbase classpath` org.apache.hadoop.hbase.util.HFileV1Detector --h
 * </ul>
 * It also supports -h, --help, -help options.
 * </p>
 */
public class HFileV1Detector extends Configured implements Tool {
  private FileSystem fs;
  private static final Log LOG = LogFactory.getLog(HFileV1Detector.class);
  private static final int DEFAULT_NUM_OF_THREADS = 10;
  private int numOfThreads;
  private Path dirToProcess;
  private final Set<Path> corruptedHFiles = Collections
      .newSetFromMap(new ConcurrentHashMap<Path, Boolean>());
  private final Set<Path> hFileV1Set = Collections
      .newSetFromMap(new ConcurrentHashMap<Path, Boolean>());

  private Options options = new Options();

  public HFileV1Detector() {
    Option pathOption = new Option("p", "path", true, "Path to a table, or hbase installation");
    pathOption.setRequired(false);
    options.addOption(pathOption);
    Option threadOption = new Option("n", "numberOfThreads", true,
        "Number of threads to use while processing HFiles.");
    threadOption.setRequired(false);
    options.addOption(threadOption);
    options.addOption("h", "help", false, "Help");
  }

  private boolean parseOption(String[] args) throws ParseException, IOException {
    if (args.length == 0) {
      return true; // no args will process with default values.
    }
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption("h")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HFileV1Detector", options, true);
      System.out
          .println("In case no option is provided, it processes hbase.rootdir using 10 threads.");
      System.out.println("Example:");
      System.out.println(" To detect any HFileV1 in a given hbase installation '/myhbase':");
      System.out.println(" $ $HBASE_HOME/bin/hbase " + this.getClass().getName() + " -p /myhbase");
      System.out.println();
      return false;
    }

    if (cmd.hasOption("p")) {
      dirToProcess = new Path(cmd.getOptionValue("p"));
    }
    try {
      if (cmd.hasOption("n")) {
        int n = Integer.parseInt(cmd.getOptionValue("n"));
        if (n < 0 || n > 100) {
          System.out.println("Please use a positive number <= 100 for number of threads."
              + " Continuing with default value " + DEFAULT_NUM_OF_THREADS);
          return true;
        }
        numOfThreads = n;
      }
    } catch (NumberFormatException nfe) {
      System.err.println("Please select a valid number for threads");
      return false;
    }
    return true;
  }

  @Override
  public int run(String args[]) throws IOException, ParseException {
    fs = FileSystem.get(getConf());
    numOfThreads = DEFAULT_NUM_OF_THREADS;
    dirToProcess = FSUtils.getRootDir(getConf());
    if (!parseOption(args)) {
      System.exit(1);
    }
    ExecutorService exec = Executors.newFixedThreadPool(numOfThreads);
    Set<Path> regionsWithHFileV1;
    try {
      regionsWithHFileV1 = checkForV1Files(dirToProcess, exec);
      printHRegionsWithHFileV1(regionsWithHFileV1);
      printAllHFileV1();
      printCorruptedHFiles();
      if (hFileV1Set.isEmpty() && corruptedHFiles.isEmpty()) {
        // all clear.
        System.out.println("No HFile V1 Found");
      }
    } catch (Exception e) {
      System.err.println(e);
      return 1;
    } finally {
      exec.shutdown();
      fs.close();
    }
    return 0;
  }

  /**
   * Takes a directory path, and lists out any HFileV1, if present.
   * @param targetDir directory to start looking for HFilev1.
   * @param exec
   * @return set of Regions that have HFileV1
   * @throws IOException
   */
  private Set<Path> checkForV1Files(Path targetDir, final ExecutorService exec) throws IOException {
    if (isTableDir(fs, targetDir)) {
      return processTable(targetDir, exec);
    }
    // user has passed a hbase installation directory.
    if (!fs.exists(targetDir)) {
      throw new IOException("The given path does not exist: " + targetDir);
    }
    Set<Path> regionsWithHFileV1 = new HashSet<Path>();
    FileStatus[] fsStats = fs.listStatus(targetDir);
    for (FileStatus fsStat : fsStats) {
      if (isTableDir(fs, fsStat.getPath())) {
        // look for regions and find out any v1 file.
        regionsWithHFileV1.addAll(processTable(fsStat.getPath(), exec));
      } else {
        LOG.info("Ignoring path: " + fsStat.getPath());
      }
    }
    return regionsWithHFileV1;
  }

  /**
   * Find out the regions in the table which has an HFile v1 in it.
   * @param tableDir
   * @param exec
   * @return the set of regions containing HFile v1.
   * @throws IOException
   */
  private Set<Path> processTable(Path tableDir, final ExecutorService exec) throws IOException {
    // list out the regions and then process each file in it.
    LOG.info("processing table: " + tableDir);
    List<Future<Path>> regionLevelResults = new ArrayList<Future<Path>>();
    Set<Path> regionsWithHFileV1 = new HashSet<Path>();

    FileStatus[] fsStats = fs.listStatus(tableDir);
    for (FileStatus fsStat : fsStats) {
      // process each region
      if (isRegionDir(fs, fsStat.getPath())) {
        regionLevelResults.add(processRegion(fsStat.getPath(), exec));
      }
    }
    for (Future<Path> f : regionLevelResults) {
      try {
        if (f.get() != null) {
          regionsWithHFileV1.add(f.get());
        }
      } catch (InterruptedException e) {
        System.err.println(e);
      } catch (ExecutionException e) {
        System.err.println(e); // might be a bad hfile. We print it at the end.
      }
    }
    return regionsWithHFileV1;
  }

  /**
   * Each region is processed by a separate handler. If a HRegion has a hfileV1, its path is
   * returned as the future result, otherwise, a null value is returned.
   * @param regionDir Region to process.
   * @param exec
   * @return corresponding Future object.
   */
  private Future<Path> processRegion(final Path regionDir, final ExecutorService exec) {
    LOG.info("processing region: " + regionDir);
    Callable<Path> regionCallable = new Callable<Path>() {
      @Override
      public Path call() throws Exception {
        for (Path familyDir : FSUtils.getFamilyDirs(fs, regionDir)) {
          FileStatus[] storeFiles = FSUtils.listStatus(fs, familyDir);
          if (storeFiles == null || storeFiles.length == 0) continue;
          for (FileStatus storeFile : storeFiles) {
            Path storeFilePath = storeFile.getPath();
            FSDataInputStream fsdis = null;
            long lenToRead = 0;
            try {
              // check whether this path is a reference.
              if (StoreFileInfo.isReference(storeFilePath)) continue;
              // check whether this path is a HFileLink.
              else if (HFileLink.isHFileLink(storeFilePath)) {
                HFileLink fileLink = new HFileLink(getConf(), storeFilePath);
                fsdis = fileLink.open(fs);
                lenToRead = fileLink.getFileStatus(fs).getLen();
              } else {
                // a regular hfile
                fsdis = fs.open(storeFilePath);
                lenToRead = storeFile.getLen();
              }
              FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, lenToRead);
              int version = trailer.getMajorVersion();
              if (version == 1) {
                hFileV1Set.add(storeFilePath);
                // return this region path, as it needs to be compacted.
                return regionDir;
              }
            } catch (Exception iae) {
              corruptedHFiles.add(storeFilePath);
            } finally {
              if (fsdis != null) fsdis.close();
            }
          }
        }
        return null;
      }
    };
    Future<Path> f = exec.submit(regionCallable);
    return f;
  }

  private static boolean isTableDir(final FileSystem fs, final Path path) throws IOException {
    return FSTableDescriptors.getTableInfoPath(fs, path) != null;
  }

  private static boolean isRegionDir(final FileSystem fs, final Path path) throws IOException {
    Path regionInfo = new Path(path, HRegionFileSystem.REGION_INFO_FILE);
    return fs.exists(regionInfo);

  }

  private void printHRegionsWithHFileV1(Set<Path> regionsHavingHFileV1) {
    if (!regionsHavingHFileV1.isEmpty()) {
      System.out.println();
      System.out.println("Following regions has HFileV1 and needs to be Major Compacted:");
      System.out.println();
      for (Path r : regionsHavingHFileV1) {
        System.out.println(r);
      }
      System.out.println();
    }
  }

  private void printAllHFileV1() {
    if (!hFileV1Set.isEmpty()) {
      System.out.println();
      System.out.println("Following HFileV1 are found:");
      System.out.println();
      for (Path r : hFileV1Set) {
        System.out.println(r);
      }
      System.out.println();
    }

  }

  private void printCorruptedHFiles() {
    if (!corruptedHFiles.isEmpty()) {
      System.out.println();
      System.out.println("Following HFiles are corrupted as their version is unknown:");
      System.out.println();
      for (Path r : corruptedHFiles) {
        System.out.println(r);
      }
      System.out.println();
    }
  }

  public static void main(String args[]) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new HFileV1Detector(), args));
  }

}
