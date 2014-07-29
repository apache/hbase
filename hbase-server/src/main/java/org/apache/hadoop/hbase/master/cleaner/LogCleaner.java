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
package org.apache.hadoop.hbase.master.cleaner;

import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;

/**
 * This Chore, every time it runs, will attempt to delete the WALs in the old logs folder. The WAL
 * is only deleted if none of the cleaner delegates says otherwise.
 * @see BaseLogCleanerDelegate
 */
@InterfaceAudience.Private
public class LogCleaner extends CleanerChore<BaseLogCleanerDelegate> {
  static final Log LOG = LogFactory.getLog(LogCleaner.class.getName());

  /**
   * @param p the period of time to sleep between each run
   * @param s the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param oldLogDir the path to the archived logs
   */
  public LogCleaner(final int p, final Stoppable s, Configuration conf, FileSystem fs,
      Path oldLogDir) {
    super("LogsCleaner", p, s, conf, fs, oldLogDir, HBASE_MASTER_LOGCLEANER_PLUGINS);
  }

  @Override
  protected boolean validate(Path file) {
    return DefaultWALProvider.validateWALFilename(file.getName());
  }
}
