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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common helpers for testing HBase that do not depend on specific server/etc. things.
 * @see org.apache.hadoop.hbase.HBaseCommonTestingUtil
 * @deprecated since 3.0.0, will be removed in 4.0.0. Use
 *             {@link org.apache.hadoop.hbase.testing.TestingHBaseCluster} instead.
 */
@InterfaceAudience.Public
@Deprecated
public class HBaseCommonTestingUtility {
  protected static final Logger LOG = LoggerFactory.getLogger(HBaseCommonTestingUtility.class);

  /**
   * Compression algorithms to use in parameterized JUnit 4 tests
   */
  public static final List<Object[]> COMPRESSION_ALGORITHMS_PARAMETERIZED =
    Arrays.asList(new Object[][] {
      { Compression.Algorithm.NONE },
      { Compression.Algorithm.GZ }
    });

  /**
   * This is for unit tests parameterized with a two booleans.
   */
  public static final List<Object[]> BOOLEAN_PARAMETERIZED =
      Arrays.asList(new Object[][] {
          {false},
          {true}
      });

  /**
   * Compression algorithms to use in testing
   */
  public static final Compression.Algorithm[] COMPRESSION_ALGORITHMS = {
    Compression.Algorithm.NONE, Compression.Algorithm.GZ
  };

  protected final Configuration conf;

  public HBaseCommonTestingUtility() {
    this(null);
  }

  public HBaseCommonTestingUtility(Configuration conf) {
    this.conf = (conf == null ? HBaseConfiguration.create() : conf);
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
   * @return Where to write test data on local filesystem, specific to the test. Useful for tests
   *    that do not use a cluster. Creates it if it does not exist already.
   */
  public Path getDataTestDir() {
    if (this.dataTestDir == null) {
      setupDataTestDir();
    }
    return new Path(this.dataTestDir.getAbsolutePath());
  }

  /**
   * @param name the name of a subdirectory or file in the test data directory
   * @return Path to a subdirectory or file named {code subdirName} under
   *  {@link #getDataTestDir()}. Does *NOT* create the directory or file if it does not exist.
   */
  public Path getDataTestDir(final String name) {
    return new Path(getDataTestDir(), name);
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

  /**
   * @return A dir with a random (uuid) name under the test dir
   * @see #getBaseTestDir()
   */
  public Path getRandomDir() {
    return new Path(getBaseTestDir(), getRandomUUID().toString());
  }

  public static UUID getRandomUUID() {
    return new UUID(ThreadLocalRandom.current().nextLong(),
                    ThreadLocalRandom.current().nextLong());
  }

  protected void createSubDir(String propertyName, Path parent, String subDirName) {
    Path newPath = new Path(parent, subDirName);
    File newDir = new File(newPath.toString()).getAbsoluteFile();

    if (deleteOnExit()) {
      newDir.deleteOnExit();
    }

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
   */
  public boolean cleanupTestDir() {
    if (deleteDir(this.dataTestDir)) {
      this.dataTestDir = null;
      return true;
    }
    return false;
  }

  /**
   * @param subdir Test subdir name.
   * @return True if we removed the test dir
   */
  public boolean cleanupTestDir(final String subdir) {
    if (this.dataTestDir == null) {
      return false;
    }
    return deleteDir(new File(this.dataTestDir, subdir));
  }

  /**
   * @return Where to write test data on local filesystem; usually
   *    {@link #DEFAULT_BASE_TEST_DIRECTORY}
   *    Should not be used by the unit tests, hence its's private.
   *    Unit test will use a subdirectory of this directory.
   * @see #setupDataTestDir()
   */
  private Path getBaseTestDir() {
    String PathName = System.getProperty(
        BASE_TEST_DIRECTORY_KEY, DEFAULT_BASE_TEST_DIRECTORY);

    return new Path(PathName);
  }

  /**
   * @param dir Directory to delete
   * @return True if we deleted it.
   */
  boolean deleteDir(final File dir) {
    if (dir == null || !dir.exists()) {
      return true;
    }
    int ntries = 0;
    do {
      ntries += 1;
      try {
        if (deleteOnExit()) {
          FileUtils.deleteDirectory(dir);
        }

        return true;
      } catch (IOException ex) {
        LOG.warn("Failed to delete " + dir.getAbsolutePath());
      } catch (IllegalArgumentException ex) {
        LOG.warn("Failed to delete " + dir.getAbsolutePath(), ex);
      }
    } while (ntries < 30);

    return false;
  }

  /**
   * Wrapper method for {@link Waiter#waitFor(Configuration, long, Predicate)}.
   */
  public <E extends Exception> long waitFor(long timeout, Predicate<E> predicate)
      throws E {
    return Waiter.waitFor(this.conf, timeout, predicate);
  }

  /**
   * Wrapper method for {@link Waiter#waitFor(Configuration, long, long, Predicate)}.
   */
  public <E extends Exception> long waitFor(long timeout, long interval, Predicate<E> predicate)
      throws E {
    return Waiter.waitFor(this.conf, timeout, interval, predicate);
  }

  /**
   * Wrapper method for {@link Waiter#waitFor(Configuration, long, long, boolean, Predicate)}.
   */
  public <E extends Exception> long waitFor(long timeout, long interval,
      boolean failIfTimeout, Predicate<E> predicate) throws E {
    return Waiter.waitFor(this.conf, timeout, interval, failIfTimeout, predicate);
  }

  private static final PortAllocator portAllocator = new PortAllocator();

  public static int randomFreePort() {
    return portAllocator.randomFreePort();
  }

  static class PortAllocator {
    private static final int MIN_RANDOM_PORT = 0xc000;
    private static final int MAX_RANDOM_PORT = 0xfffe;

    /** A set of ports that have been claimed using {@link #randomFreePort()}. */
    private final Set<Integer> takenRandomPorts = new HashSet<>();

    private final AvailablePortChecker portChecker;

    public PortAllocator() {
      this.portChecker = new AvailablePortChecker() {
        @Override
        public boolean available(int port) {
          try {
            ServerSocket sock = new ServerSocket(port);
            sock.close();
            return true;
          } catch (IOException ex) {
            return false;
          }
        }
      };
    }

    public PortAllocator(AvailablePortChecker portChecker) {
      this.portChecker = portChecker;
    }

    /**
     * Returns a random free port and marks that port as taken. Not thread-safe. Expected to be
     * called from single-threaded test setup code/
     */
    public int randomFreePort() {
      int port = 0;
      do {
        port = randomPort();
        if (takenRandomPorts.contains(port)) {
          port = 0;
          continue;
        }
        takenRandomPorts.add(port);

        if (!portChecker.available(port)) {
          port = 0;
        }
      } while (port == 0);
      return port;
    }

    /**
     * Returns a random port. These ports cannot be registered with IANA and are
     * intended for dynamic allocation (see http://bit.ly/dynports).
     */
    private int randomPort() {
      return MIN_RANDOM_PORT
        + ThreadLocalRandom.current().nextInt(MAX_RANDOM_PORT - MIN_RANDOM_PORT);
    }

    interface AvailablePortChecker {
      boolean available(int port);
    }
  }
}
