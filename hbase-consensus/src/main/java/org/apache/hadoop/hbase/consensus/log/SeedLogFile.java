package org.apache.hadoop.hbase.consensus.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.hbase.HConstants;

import java.io.File;
import java.io.IOException;

/**
 * This is a special log file used to denote the seed Index for the transaction
 * log manager.
 */
public class SeedLogFile extends ReadOnlyLog {

  public SeedLogFile(final File file) {
    super(file, HConstants.SEED_TERM, getInitialSeedIndex(file));
  }

  /**
   * Remove and close the registered reader from the reader map by the session key
   * @param sessionKey
   */
  public void removeReader(String sessionKey) throws IOException {
    // Do-nothing
  }

  /**
   * Close all the LogReader instances and delete the file
   * @throws IOException
   */
  @Override
  public void closeAndDelete() throws IOException {
    super.closeAndDelete();
  }

  @Override public long getTxnCount() {
    return lastIndex - initialIndex + 1;
  }

  public static boolean isSeedFile(final File f) {
    return isSeedFile(f.getName());
  }

  public static boolean isSeedFile(String fileName) {
    String[] split = fileName.split("_");
    return split[0].equals("-2");
  }

  public static long getInitialSeedIndex(final File f) {
    final String[] split = f.getName().split("_");
    return Long.parseLong(split[1]);
  }
}
