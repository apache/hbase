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
import java.io.EOFException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.hbase.io.SequenceFile;

/** Tries to read the file created by Writer */
public class Reader {

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1 || args.length > 2) {
      System.err.println("usage: Reader expected-number-of-records [ -n ]");
      System.err.println("              where -n = do not try to recover lease");
      return;
    }
    int expected = Integer.valueOf(args[0]);
    boolean recover = true;
    if (args.length == 2 && args[1].compareTo("-n") == 0) {
      recover = false;
    }

    Configuration conf = new Configuration();
    Path dir = new Path(conf.get("fs.default.name"), "log"); 
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("Wrong file system: " + fs.getClass().getName());
    }

    if (recover) {
      waitForLeaseRecovery(fs, new Path(dir, "log"));
    }
    
    SequenceFile.Reader in = null;
    try {
      in = new SequenceFile.Reader(fs, new Path(dir, "log"), conf);
    } catch (EOFException e) {
      if (expected != 0) {
        e.printStackTrace();
      }
      return;
    }
    
    IntWritable key = new IntWritable();
    BytesWritable value = new BytesWritable();
    int count = 0;
    IOException ex = null;
    try {
        while (in.next(key, value)) {
        count++;
      }
    } catch (IOException e) {
      ex = e;
    }
    if (expected != count) {
      System.err.println("Read " + count + " lines, expected " + expected +
          " lines");
    }
    in.close();
    if (ex != null) {
      ex.printStackTrace();
    }
  }

  static void waitForLeaseRecovery(FileSystem fs, Path file) {
    boolean done = false;
    while (!done) {
      try {
        Thread.sleep(10*1000);
      } catch (InterruptedException e) {
        System.out.println("Sleep interrupted.");
      }
      try {
        FSDataOutputStream out = fs.append(file);
        out.close();
        done = true;
      } catch (IOException e) {
        System.out.println("Triggering lease recovery if needed.");
      }
    }
    System.out.println("Lease Recovery Successful");
  }
}
