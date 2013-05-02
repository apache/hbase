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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;

/**
 * Acts as an abstraction between the HLog and the underlying filesystem. This is analogous to the
 * {@link HRegionFileSystem} class.
 */
public class HLogFileSystem extends HBaseFileSystem {
  public static final Log LOG = LogFactory.getLog(HLogFileSystem.class);

  /**
   * In order to handle NN connectivity hiccups, one need to retry non-idempotent operation at the
   * client level.
   */

  public HLogFileSystem(Configuration conf) {
    setRetryCounts(conf);
  }

  /**
   * Creates writer for the given path.
   * @param fs
   * @param conf
   * @param hlogFile
   * @return an init'ed writer for the given path.
   * @throws IOException
   */
  public Writer createWriter(FileSystem fs, Configuration conf, Path hlogFile) throws IOException {
    int i = 0;
    IOException lastIOE = null;
    do {
      try {
        return HLog.createWriter(fs, hlogFile, conf);
      } catch (IOException ioe) {
        lastIOE = ioe;
        sleepBeforeRetry("Create Writer", i+1);
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createWriter", lastIOE);

  }
}
