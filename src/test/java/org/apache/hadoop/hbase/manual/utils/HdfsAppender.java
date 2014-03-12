/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.manual.utils;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.HasThread;

/**
 *  Runs a continuous loop, appending files to measure perf
 */
public class HdfsAppender extends HasThread
{
  private static final Log LOG = LogFactory.getLog(HdfsAppender.class);

  final int minSize;
  final int maxSize;
  final HBaseConfiguration conf;
  
  public HdfsAppender(HBaseConfiguration conf, int minSize, int maxSize) {
    this.conf = conf;
    this.minSize = minSize;
    this.maxSize = maxSize;
  }

  @Override
  public void run() {
    Random r = new Random();
    byte[] t = new byte[] {'t','e','s','t'};
    HLogKey key = new HLogKey(t, t, 0, 0);
    
    try {
      FileSystem fs = FileSystem.get(conf);
      int fileCount = 0;
  
      long blocksize = conf.getLong("hbase.regionserver.hlog.blocksize",
          fs.getDefaultBlockSize());
      // Roll at 95% of block size.
      float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
      long logrollsize = (long)(blocksize * multi);
      
      while(true) {
        Path p = new Path("/appendtest", Integer.toString(fileCount));
        HLog.Writer writer = HLog.createWriter(fs, p, conf);
        
        while (writer.getLength() < logrollsize) {
          int rSize = r.nextInt(maxSize-minSize) + minSize;
          WALEdit value = new WALEdit();
          value.add(new KeyValue(t, t, t, 0, new byte[rSize]));
    
          long now = System.currentTimeMillis();
          writer.append(new HLog.Entry(key, value));
          long took = System.currentTimeMillis() - now;
          LOG.info(String.format("append time (%d) = %d ms", rSize, took));
          
          now = System.currentTimeMillis();
          writer.sync();
          took = System.currentTimeMillis() - now;
          LOG.info(String.format("sync time = %d ms", took));
        }
        LOG.info(String.format("rolling logs to %d", fileCount));

        writer.close();
        fs.delete(p, false);
        ++fileCount;
      }
    } catch (IOException ioe) {
      LOG.error("ioe encountered! stopping append thread", ioe);
    }
  }
}
