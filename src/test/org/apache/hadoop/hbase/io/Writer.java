/**
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.hbase.io.SequenceFile;

/** Writes to a Sequence file and then commits suicide */
public class Writer {
  private static byte[] bytes = new byte[1020];
  private static BytesWritable value;

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.err.println("usage: Writer total-writes writes-per-sync block-size-mb");
      return;
    }
    long blocksize = Long.valueOf(args[2]);
    if (blocksize != 1L && blocksize != 64L) {
      System.err.println("Only 1MB and 64MB blocksizes are allowed");
      return;
    }
    blocksize *= 1024L * 1024L;
    
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte)(i % 64);
    }
    value = new BytesWritable(bytes);
    
    Configuration conf = new Configuration();
    Path dir = new Path(conf.get("fs.default.name"), "log"); 
    conf.set("fs.default.name", dir.toString());
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("Wrong file system: " + fs.getClass().getName());
    }
    
    fs.mkdirs(dir);
    Path file = new Path(dir, "log");
    if (fs.exists(file)) {
      fs.delete(file, false);
    }
    
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, file,
        IntWritable.class, BytesWritable.class,
        fs.getConf().getInt("io.file.buffer.size", 4096),
        fs.getDefaultReplication(), blocksize,
        SequenceFile.CompressionType.NONE, new DefaultCodec(), null,
        new SequenceFile.Metadata());
    
    int totalWrites = Integer.valueOf(args[0]);
    int writesPerSync = Integer.valueOf(args[1]);
    for (int i = 1; i <= totalWrites; i++) {
      writer.append(new IntWritable(i), value);
      if (i % writesPerSync == 0) {
        writer.syncFs();
      }
    }

    // The following *should* prevent hdfs shutdown hook from running
    Runtime.getRuntime().halt(-1);
  }
}
