/**
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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Common helpers for testing HBase that do not depend on specific server/etc. things.
 * @see {@link HBaseTestingUtility}
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class HBaseCommonTestingUtility {
  protected static final Log LOG = LogFactory.getLog(HBaseCommonTestingUtility.class);

  protected Configuration conf;

  public HBaseCommonTestingUtility() {
    this(HBaseConfiguration.create());
  }

  public HBaseCommonTestingUtility(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Returns this classes's instance of {@link Configuration}.
   *
   * @return Instance of Configuration.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * System property key to get base test directory value
   */
  public static final String BASE_TEST_DIRECTORY_KEY =
      "test.build.data.basedirectory";

  /**
   * Default base directory for test output.
   */
  public static final String DEFAULT_BASE_TEST_DIRECTORY = "target/test-data";

  /**
   * Directory where we put the data for this instance of HBaseTestingUtility
   */
  private File dataTestDir = null;

  /**
   * @return Where to write test data on local filesystem, specific to
   * the test.  Useful for tests that do not use a cluster.
   * Creates it if it does not exist already.
   */
  public Path getDataTestDir() {
    if (this.dataTestDir == null) {
      setupDataTestDir();
    }
    return new Path(this.dataTestDir.getAbsolutePath());
  }

  /**
   * @param subdirName
   * @return Path to a subdirectory named <code>subdirName</code> under
   * {@link #getDataTestDir()}.
   * Does *NOT* create it if it does not exist.
   */
  public Path getDataTestDir(final String subdirName) {
    return new Path(getDataTestDir(), subdirName);
  }

  /**
   * Sets up a directory for a test to use.
   *
   * @return New directory path, if created.
   */
  protected Path setupDataTestDir() {
    if (this.dataTestDir != null) {
      LOG.warn("Data test dir already setup in " +
          dataTestDir.getAbsolutePath());
      return null;
    }

    String randomStr = UUID.randomUUID().toString();
    Path testPath = new Path(getBaseTestDir(), randomStr);

    this.dataTestDir = new File(testPath.toString()).getAbsoluteFile();
    // Set this property so if mapreduce jobs run, they will use this as their home dir.
    System.setProperty("test.build.dir", this.dataTestDir.toString());
    if (deleteOnExit()) this.dataTestDir.deleteOnExit();

    createSubDir("hbase.local.dir", testPath, "hbase-local-dir");

    return testPath;
  }

  protected void createSubDir(String propertyName, Path parent, String subDirName) {
    Path newPath = new Path(parent, subDirName);
    File newDir = new File(newPath.toString()).getAbsoluteFile();
    if (deleteOnExit()) newDir.deleteOnExit();
    conf.set(propertyName, newDir.getAbsolutePath());
  }

  /**
   * @return True if we should delete testing dirs on exit.
   */
  boolean deleteOnExit() {
    String v = System.getProperty("hbase.testing.preserve.testdir");
    // Let default be true, to delete on exit.
    return v == null ? true : !Boolean.parseBoolean(v);
  }

  /**
   * @return True if we removed the test dirs
   * @throws IOException
   */
  public boolean cleanupTestDir() throws IOException {
    if (deleteDir(this.dataTestDir)) {
      this.dataTestDir = null;
      return true;
    }
    return false;
  }

  /**
   * @param subdir Test subdir name.
   * @return True if we removed the test dir
   * @throws IOException
   */
  boolean cleanupTestDir(final String subdir) throws IOException {
    if (this.dataTestDir == null) {
      return false;
    }
    return deleteDir(new File(this.dataTestDir, subdir));
  }

  /**
   * @return Where to write test data on local filesystem; usually
   * {@link #DEFAULT_BASE_TEST_DIRECTORY}
   * Should not be used by the unit tests, hence its's private.
   * Unit test will use a subdirectory of this directory.
   * @see #setupDataTestDir()
   */
  private Path getBaseTestDir() {
    String PathName = System.getProperty(
        BASE_TEST_DIRECTORY_KEY, DEFAULT_BASE_TEST_DIRECTORY);

    return new Path(PathName);
  }

  public Path getRandomDir() {
    String randomStr = UUID.randomUUID().toString();
    Path testPath = new Path(getBaseTestDir(), randomStr);
    return testPath;
  }

  /**
   * @param dir Directory to delete
   * @return True if we deleted it.
   * @throws IOException
   */
  boolean deleteDir(final File dir) throws IOException {
    if (dir == null || !dir.exists()) {
      return true;
    }
    int ntries = 0;
    do {
      ntries += 1;
      try {
        if (deleteOnExit()) FileUtils.deleteDirectory(dir);
        return true;
      } catch (IOException ex) {
        LOG.warn("Failed to delete " + dir.getAbsolutePath());
      } catch (IllegalArgumentException ex) {
        LOG.warn("Failed to delete " + dir.getAbsolutePath(), ex);
      }
    } while (ntries < 30);
    return ntries < 30;
  }
}
