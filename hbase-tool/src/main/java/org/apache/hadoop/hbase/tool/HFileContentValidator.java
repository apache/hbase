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
package org.apache.hadoop.hbase.tool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class HFileContentValidator extends AbstractHBaseTool {

  private static final Logger LOG = LoggerFactory.getLogger(HFileContentValidator.class);

  /**
   * Check HFile contents are readable by HBase 2.
   *
   * @param conf used configuration
   * @return number of HFiles corrupted HBase
   * @throws IOException if a remote or network exception occurs
   */
  private boolean validateHFileContent(Configuration conf) throws IOException {
    FileSystem fileSystem = CommonFSUtils.getCurrentFileSystem(conf);

    ExecutorService threadPool = createThreadPool(conf);
    HFileCorruptionChecker checker;

    try {
      checker = new HFileCorruptionChecker(conf, threadPool, false);

      Path rootDir = CommonFSUtils.getRootDir(conf);
      LOG.info("Validating HFile contents under {}", rootDir);

      Collection<Path> tableDirs = FSUtils.getTableDirs(fileSystem, rootDir);
      checker.checkTables(tableDirs);

      Path archiveRootDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
      LOG.info("Validating HFile contents under {}", archiveRootDir);

      List<Path> archiveTableDirs = FSUtils.getTableDirs(fileSystem, archiveRootDir);
      checker.checkTables(archiveTableDirs);
    } finally {
      threadPool.shutdown();

      try {
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    int checkedFiles = checker.getHFilesChecked();
    Collection<Path> corrupted = checker.getCorrupted();

    if (corrupted.isEmpty()) {
      LOG.info("Checked {} HFiles, none of them are corrupted.", checkedFiles);
      LOG.info("There are no incompatible HFiles.");

      return true;
    } else {
      LOG.info("Checked {} HFiles, {} are corrupted.", checkedFiles, corrupted.size());

      for (Path path : corrupted) {
        LOG.info("Corrupted file: {}", path);
      }

      LOG.info("Change data block encodings before upgrading. "
          + "Check https://s.apache.org/prefixtree for instructions.");

      return false;
    }
  }

  private ExecutorService createThreadPool(Configuration conf) {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    int numThreads = conf.getInt("hfilevalidator.numthreads", availableProcessors);
    return Executors.newFixedThreadPool(numThreads,
      new ThreadFactoryBuilder().setNameFormat("hfile-validator-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
  }

  @Override
  protected void addOptions() {
  }

  @Override
  protected void processOptions(CommandLine cmd) {
  }

  @Override
  protected int doWork() throws Exception {
    return (validateHFileContent(getConf())) ? EXIT_SUCCESS : EXIT_FAILURE;
  }
}
