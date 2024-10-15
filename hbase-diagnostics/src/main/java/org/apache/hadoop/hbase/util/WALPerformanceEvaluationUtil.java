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
package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains code copied from HBaseTestingUtil and its super classes required by
 * WALPerformanceEvaluation. This was done as part of refactoring for hbase-diagnostics module
 * creation in HBASE-28432 to break cyclic dependency.
 */
@InterfaceAudience.Private
public class WALPerformanceEvaluationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(WALPerformanceEvaluationUtil.class);

  /**
   * Directory on test filesystem where we put the data for this instance of
   * WALPerformanceEvaluationUtil
   */
  private Path dataTestDirOnTestFS = null;
  /**
   * Directory where we put the data for this instance of WALPerformanceEvaluationUtil
   */
  private File dataTestDir = null;
  /**
   * System property key to get base test directory value
   */
  private static final String BASE_TEST_DIRECTORY_KEY = "test.build.data.basedirectory";

  /**
   * Default base directory for test output.
   */
  private static final String DEFAULT_BASE_TEST_DIRECTORY = "target/test-data";

  private Configuration conf;

  public WALPerformanceEvaluationUtil(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @return Where to write test data on local filesystem; usually
   *         {@link #DEFAULT_BASE_TEST_DIRECTORY} Should not be used by the unit tests, hence its's
   *         private. Unit test will use a subdirectory of this directory.
   * @see #setupDataTestDir()
   */
  private Path getBaseTestDir() {
    String PathName = System.getProperty(BASE_TEST_DIRECTORY_KEY, DEFAULT_BASE_TEST_DIRECTORY);

    return new Path(PathName);
  }

  private static UUID getRandomUUID() {
    return new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong());
  }

  /**
   * @return A dir with a random (uuid) name under the test dir
   * @see #getBaseTestDir()
   */
  private Path getRandomDir() {
    return new Path(getBaseTestDir(), getRandomUUID().toString());
  }

  private void createSubDir(String propertyName, Path parent, String subDirName) {
    Path newPath = new Path(parent, subDirName);
    File newDir = new File(newPath.toString()).getAbsoluteFile();

    if (deleteOnExit()) {
      newDir.deleteOnExit();
    }

    conf.set(propertyName, newDir.getAbsolutePath());
  }

  /**
   * Home our data in a dir under {@link #DEFAULT_BASE_TEST_DIRECTORY}. Give it a random name so can
   * have many concurrent tests running if we need to. Moding a System property is not the way to do
   * concurrent instances -- another instance could grab the temporary value unintentionally -- but
   * not anything can do about it at moment; single instance only is how the minidfscluster works.
   * We also create the underlying directory names for hadoop.log.dir, mapreduce.cluster.local.dir
   * and hadoop.tmp.dir, and set the values in the conf, and as a system property for hadoop.tmp.dir
   * (We do not create them!).
   * @return The calculated data test build directory, if newly-created.
   */
  protected Path setupDataTestDir() {
    Path testPath = setupDataTestDirInternal();
    if (null == testPath) {
      return null;
    }

    createSubDirAndSystemProperty("hadoop.log.dir", testPath, "hadoop-log-dir");

    // This is defaulted in core-default.xml to /tmp/hadoop-${user.name}, but
    // we want our own value to ensure uniqueness on the same machine
    createSubDirAndSystemProperty("hadoop.tmp.dir", testPath, "hadoop-tmp-dir");

    // Read and modified in org.apache.hadoop.mapred.MiniMRCluster
    createSubDir("mapreduce.cluster.local.dir", testPath, "mapred-local-dir");
    return testPath;
  }

  /**
   * Sets up a directory for a test to use.
   * @return New directory path, if created.
   */
  private Path setupDataTestDirInternal() {
    if (this.dataTestDir != null) {
      LOG.warn("Data test dir already setup in " + dataTestDir.getAbsolutePath());
      return null;
    }
    Path testPath = getRandomDir();
    this.dataTestDir = new File(testPath.toString()).getAbsoluteFile();
    // Set this property so if mapreduce jobs run, they will use this as their home dir.
    System.setProperty("test.build.dir", this.dataTestDir.toString());

    if (deleteOnExit()) {
      this.dataTestDir.deleteOnExit();
    }

    createSubDir("hbase.local.dir", testPath, "hbase-local-dir");

    return testPath;
  }

  private void createSubDirAndSystemProperty(String propertyName, Path parent, String subDirName) {

    String sysValue = System.getProperty(propertyName);

    if (sysValue != null) {
      // There is already a value set. So we do nothing but hope
      // that there will be no conflicts
      LOG.info("System.getProperty(\"" + propertyName + "\") already set to: " + sysValue
        + " so I do NOT create it in " + parent);
      String confValue = conf.get(propertyName);
      if (confValue != null && !confValue.endsWith(sysValue)) {
        LOG.warn(propertyName + " property value differs in configuration and system: "
          + "Configuration=" + confValue + " while System=" + sysValue
          + " Erasing configuration value by system value.");
      }
      conf.set(propertyName, sysValue);
    } else {
      // Ok, it's not set, so we create it as a subdirectory
      createSubDir(propertyName, parent, subDirName);
      System.setProperty(propertyName, conf.get(propertyName));
    }
  }

  private FileSystem getTestFileSystem() throws IOException {
    return HFileSystem.get(conf);
  }

  /**
   * @return Where to write test data on the test filesystem; Returns working directory for the test
   *         filesystem by default
   * @see #setupDataTestDirOnTestFS()
   * @see #getTestFileSystem()
   */
  private Path getBaseTestDirOnTestFS() throws IOException {
    FileSystem fs = getTestFileSystem();
    return new Path(fs.getWorkingDirectory(), "test-data");
  }

  /**
   * Returns True if we should delete testing dirs on exit.
   */
  private boolean deleteOnExit() {
    String v = System.getProperty("hbase.testing.preserve.testdir");
    // Let default be true, to delete on exit.
    return v == null ? true : !Boolean.parseBoolean(v);
  }

  /**
   * @return Where to write test data on local filesystem, specific to the test. Useful for tests
   *         that do not use a cluster. Creates it if it does not exist already.
   */
  private Path getDataTestDir() {
    if (this.dataTestDir == null) {
      setupDataTestDir();
    }
    return new Path(this.dataTestDir.getAbsolutePath());
  }

  /**
   * Sets up a new path in test filesystem to be used by tests.
   */
  private Path getNewDataTestDirOnTestFS() throws IOException {
    // The file system can be either local, mini dfs, or if the configuration
    // is supplied externally, it can be an external cluster FS. If it is a local
    // file system, the tests should use getBaseTestDir, otherwise, we can use
    // the working directory, and create a unique sub dir there
    FileSystem fs = getTestFileSystem();
    Path newDataTestDir;
    String randomStr = getRandomUUID().toString();
    if (fs.getUri().getScheme().equals(FileSystem.getLocal(conf).getUri().getScheme())) {
      newDataTestDir = new Path(getDataTestDir(), randomStr);
      File dataTestDir = new File(newDataTestDir.toString());
      if (deleteOnExit()) {
        dataTestDir.deleteOnExit();
      }
    } else {
      Path base = getBaseTestDirOnTestFS();
      newDataTestDir = new Path(base, randomStr);
      if (deleteOnExit()) {
        fs.deleteOnExit(newDataTestDir);
      }
    }
    return newDataTestDir;
  }

  /**
   * Sets up a path in test filesystem to be used by tests. Creates a new directory if not already
   * setup.
   */
  private void setupDataTestDirOnTestFS() throws IOException {
    if (dataTestDirOnTestFS != null) {
      LOG.warn("Data test on test fs dir already setup in " + dataTestDirOnTestFS.toString());
      return;
    }
    dataTestDirOnTestFS = getNewDataTestDirOnTestFS();
  }

  /**
   * Returns a Path in the test filesystem, obtained from {@link #getTestFileSystem()} to write
   * temporary test data. Call this method after setting up the mini dfs cluster if the test relies
   * on it.
   * @return a unique path in the test filesystem
   */
  private Path getDataTestDirOnTestFS() throws IOException {
    if (dataTestDirOnTestFS == null) {
      setupDataTestDirOnTestFS();
    }

    return dataTestDirOnTestFS;
  }

  /**
   * Returns a Path in the test filesystem, obtained from {@link #getTestFileSystem()} to write
   * temporary test data. Call this method after setting up the mini dfs cluster if the test relies
   * on it.
   * @param subdirName name of the subdir to create under the base test dir
   * @return a unique path in the test filesystem
   */
  public Path getDataTestDirOnTestFS(final String subdirName) throws IOException {
    return new Path(getDataTestDirOnTestFS(), subdirName);
  }
}
