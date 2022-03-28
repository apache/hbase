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

package org.apache.hadoop.hbase.chaos.actions;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action corrupts HFiles with a certain chance.
 */
public class CorruptDataFilesAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(CorruptDataFilesAction.class);
  private final float chance;

  /**
   * Corrupts HFiles with a certain chance
   * @param chance chance to corrupt any give data file (0.5 => 50%)
   */
  public CorruptDataFilesAction(float chance) {
    this.chance = chance * 100;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    getLogger().info("Start corrupting data files");

    FileSystem fs = CommonFSUtils.getRootDirFileSystem(getConf());
    Path rootDir = CommonFSUtils.getRootDir(getConf());
    Path defaultDir = rootDir.suffix("/data/default");
    RemoteIterator<LocatedFileStatus> iterator =  fs.listFiles(defaultDir, true);
    Random rand = ThreadLocalRandom.current();
    while (iterator.hasNext()){
      LocatedFileStatus status = iterator.next();
      if(!HFile.isHFileFormat(fs, status.getPath())){
        continue;
      }
      if ((100 * rand.nextFloat()) > chance){
        continue;
      }
      FSDataOutputStream out = fs.create(status.getPath(), true);
      try {
        out.write(0);
      } finally {
        out.close();
      }
      getLogger().info("Corrupting {}", status.getPath());
    }
    getLogger().info("Done corrupting data files");
  }

}
