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
import java.util.Arrays;
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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.io.HFileLink;
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
  /**
   * Pre-namespace archive directory
   */
  private static final String PRE_NS_DOT_ARCHIVE = ".archive";
  /**
   * Pre-namespace tmp directory
   */
  private static final String PRE_NS_DOT_TMP = ".tmp";
  private int numOfThreads;
  /**
   * directory to start the processing.
   */
  private Path targetDirPath;
  /**
   * executor for processing regions.
   */
  private ExecutorService exec;

  /**
   * Keeps record of processed tables.
   */
  private final Set<Path> processedTables = new HashSet<Path>();
  /**
   * set of corrupted HFiles (with undetermined major version)
   */
  private final Set<Path> corruptedHFiles = Collections
      .newSetFromMap(new ConcurrentHashMap<Path, Boolean>());
  /**
   * set of HfileV1;
   */
  private final Set<Path> hFileV1Set = Collections
      .newSetFromMap(new ConcurrentHashMap<Path, Boolean>());

  private Options options = new Options();
  /**
   * used for computing pre-namespace paths for hfilelinks
   */
  private Path defaultNamespace;

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
      this.targetDirPath = new Path(FSUtils.getRootDir(getConf()), cmd.getOptionValue("p"));
    }
    try {
      if (cmd.hasOption("n")) {
        int n = Integer.parseInt(cmd.getOptionValue("n"));
        if (n < 0 || n > 100) {
          LOG.warn("Please use a positive number <= 100 for number of threads."
              + " Continuing with default value " + DEFAULT_NUM_OF_THREADS);
          return true;
        }
        this.numOfThreads = n;
      }
    } catch (NumberFormatException nfe) {
      LOG.error("Please select a valid number for threads");
      return false;
    }
    return true;
  }

  /**
   * Checks for HFileV1.
   * @return 0 when no HFileV1 is present.
   *         1 when a HFileV1 is present or, when there is a file with corrupt major version
   *          (neither V1 nor V2).
   *        -1 in case of any error/exception
   */
  @Override
  public int run(String args[]) throws IOException, ParseException {
    FSUtils.setFsDefault(getConf(), new Path(FSUtils.getRootDir(getConf()).toUri()));
    fs = FileSystem.get(getConf());
    numOfThreads = DEFAULT_NUM_OF_THREADS;
    targetDirPath = FSUtils.getRootDir(getConf());
    if (!parseOption(args)) {
      System.exit(-1);
    }
    this.exec = Executors.newFixedThreadPool(numOfThreads);
    try {
      return processResult(checkForV1Files(targetDirPath));
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      exec.shutdown();
      fs.close();
    }
    return -1;
  }

  private void setDefaultNamespaceDir() throws IOException {
    Path dataDir = new Path(FSUtils.getRootDir(getConf()), HConstants.BASE_NAMESPACE_DIR);
    defaultNamespace = new Path(dataDir, NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
  }

  private int processResult(Set<Path> regionsWithHFileV1) {
    LOG.info("Result: \n");
    printSet(processedTables, "Tables Processed: ");

    int count = hFileV1Set.size();
    LOG.info("Count of HFileV1: " + count);
    if (count > 0) printSet(hFileV1Set, "HFileV1:");

    count = corruptedHFiles.size();
    LOG.info("Count of corrupted files: " + count);
    if (count > 0) printSet(corruptedHFiles, "Corrupted Files: ");

    count = regionsWithHFileV1.size();
    LOG.info("Count of Regions with HFileV1: " + count);
    if (count > 0) printSet(regionsWithHFileV1, "Regions to Major Compact: ");

    return (hFileV1Set.isEmpty() && corruptedHFiles.isEmpty()) ? 0 : 1;
  }

  private void printSet(Set<Path> result, String msg) {
    LOG.info(msg);
    for (Path p : result) {
      LOG.info(p);
    }
  }

  /**
   * Takes a directory path, and lists out any HFileV1, if present.
   * @param targetDir directory to start looking for HFilev1.
   * @return set of Regions that have HFileV1
   * @throws IOException
   */
  private Set<Path> checkForV1Files(Path targetDir) throws IOException {
    LOG.info("Target dir is: " + targetDir);
    if (!fs.exists(targetDir)) {
      throw new IOException("The given path does not exist: " + targetDir);
    }
    if (isTableDir(fs, targetDir)) {
      processedTables.add(targetDir);
      return processTable(targetDir);
    }
    Set<Path> regionsWithHFileV1 = new HashSet<Path>();
    FileStatus[] fsStats = fs.listStatus(targetDir);
    for (FileStatus fsStat : fsStats) {
      if (isTableDir(fs, fsStat.getPath()) && !isRootTable(fsStat.getPath())) {
        processedTables.add(fsStat.getPath());
        // look for regions and find out any v1 file.
        regionsWithHFileV1.addAll(processTable(fsStat.getPath()));
      } else {
        LOG.info("Ignoring path: " + fsStat.getPath());
      }
    }
    return regionsWithHFileV1;
  }

  /**
   * Ignore ROOT table as it doesn't exist in 0.96.
   * @param path
   */
  private boolean isRootTable(Path path) {
    if (path != null && path.toString().endsWith("-ROOT-")) return true;
    return false;
  }

  /**
   * Find out regions in the table which have HFileV1.
   * @param tableDir
   * @return the set of regions containing HFile v1.
   * @throws IOException
   */
  private Set<Path> processTable(Path tableDir) throws IOException {
    // list out the regions and then process each file in it.
    LOG.debug("processing table: " + tableDir);
    List<Future<Path>> regionLevelResults = new ArrayList<Future<Path>>();
    Set<Path> regionsWithHFileV1 = new HashSet<Path>();

    FileStatus[] fsStats = fs.listStatus(tableDir);
    for (FileStatus fsStat : fsStats) {
      // process each region
      if (isRegionDir(fs, fsStat.getPath())) {
        regionLevelResults.add(processRegion(fsStat.getPath()));
      }
    }
    for (Future<Path> f : regionLevelResults) {
      try {
        if (f.get() != null) {
          regionsWithHFileV1.add(f.get());
        }
      } catch (InterruptedException e) {
        LOG.error(e);
      } catch (ExecutionException e) {
        LOG.error(e); // might be a bad hfile. We print it at the end.
      }
    }
    return regionsWithHFileV1;
  }

  /**
   * Each region is processed by a separate handler. If a HRegion has a hfileV1, its path is
   * returned as the future result, otherwise, a null value is returned.
   * @param regionDir Region to process.
   * @return corresponding Future object.
   */
  private Future<Path> processRegion(final Path regionDir) {
    LOG.debug("processing region: " + regionDir);
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
                FileLink fLink = getFileLinkWithPreNSPath(storeFilePath);
                fsdis = fLink.open(fs);
                lenToRead = fLink.getFileStatus(fs).getLen();
              } else {
                // a regular hfile
                fsdis = fs.open(storeFilePath);
                lenToRead = storeFile.getLen();
              }
              int majorVersion = computeMajorVersion(fsdis, lenToRead);
              if (majorVersion == 1) {
                hFileV1Set.add(storeFilePath);
                // return this region path, as it needs to be compacted.
                return regionDir;
              }
              if (majorVersion > 2 || majorVersion < 1) throw new IllegalArgumentException(
                  "Incorrect major version: " + majorVersion);
            } catch (Exception iae) {
              corruptedHFiles.add(storeFilePath);
              LOG.error("Got exception while reading trailer for file: "+ storeFilePath, iae);
            } finally {
              if (fsdis != null) fsdis.close();
            }
          }
        }
        return null;
      }

      private int computeMajorVersion(FSDataInputStream istream, long fileSize)
       throws IOException {
        //read up the last int of the file. Major version is in the last 3 bytes.
        long seekPoint = fileSize - Bytes.SIZEOF_INT;
        if (seekPoint < 0)
          throw new IllegalArgumentException("File too small, no major version found");

        // Read the version from the last int of the file.
        istream.seek(seekPoint);
        int version = istream.readInt();
        // Extract and return the major version
        return version & 0x00ffffff;
      }
    };
    Future<Path> f = exec.submit(regionCallable);
    return f;
  }

  /**
   * Creates a FileLink which adds pre-namespace paths in its list of available paths. This is used
   * when reading a snapshot file in a pre-namespace file layout, for example, while upgrading.
   * @param storeFilePath
   * @return a FileLink which could read from pre-namespace paths.
   * @throws IOException
   */
  public FileLink getFileLinkWithPreNSPath(Path storeFilePath) throws IOException {
    HFileLink link = HFileLink.buildFromHFileLinkPattern(getConf(), storeFilePath);
    List<Path> pathsToProcess = getPreNSPathsForHFileLink(link);
    pathsToProcess.addAll(Arrays.asList(link.getLocations()));
    return new FileLink(pathsToProcess);
  }

  private List<Path> getPreNSPathsForHFileLink(HFileLink fileLink) throws IOException {
    if (defaultNamespace == null) setDefaultNamespaceDir();
    List<Path> p = new ArrayList<Path>();
    String relativeTablePath = removeDefaultNSPath(fileLink.getOriginPath());
    p.add(getPreNSPath(PRE_NS_DOT_ARCHIVE, relativeTablePath));
    p.add(getPreNSPath(PRE_NS_DOT_TMP, relativeTablePath));
    p.add(getPreNSPath(null, relativeTablePath));
    return p;
  }

  /**
   * Removes the prefix of defaultNamespace from the path.
   * @param originalPath
   */
  private String removeDefaultNSPath(Path originalPath) {
    String pathStr = originalPath.toString();
    if (!pathStr.startsWith(defaultNamespace.toString())) return pathStr;
    return pathStr.substring(defaultNamespace.toString().length() + 1);
  }

  private Path getPreNSPath(String prefix, String relativeTablePath) throws IOException {
    String relativePath = (prefix == null ? relativeTablePath : prefix + Path.SEPARATOR
        + relativeTablePath);
    return new Path(FSUtils.getRootDir(getConf()), relativePath);
  }

  private static boolean isTableDir(final FileSystem fs, final Path path) throws IOException {
    // check for old format, of having /table/.tableinfo; hbase:meta doesn't has .tableinfo,
    // include it.
    if (fs.isFile(path)) return false;
    return (FSTableDescriptors.getTableInfoPath(fs, path) != null || FSTableDescriptors
        .getCurrentTableInfoStatus(fs, path, false) != null) || path.toString().endsWith(".META.");
  }

  private static boolean isRegionDir(final FileSystem fs, final Path path) throws IOException {
    if (fs.isFile(path)) return false;
    Path regionInfo = new Path(path, HRegionFileSystem.REGION_INFO_FILE);
    return fs.exists(regionInfo);

  }

  public static void main(String args[]) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new HFileV1Detector(), args));
  }

}
