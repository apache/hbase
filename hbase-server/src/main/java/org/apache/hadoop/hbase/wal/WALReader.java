/*
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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Read the passed in WAL passed on command-line (or recovered.edits file since it has same
 * format). Stamps out offset, key, and value size of edit seen. Use to verify WAL or
 * recovered.edits files.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class WALReader {
  /**
   * Private constructor.
   */
  private WALReader() {}

  private static void read(WALFactory factory, FileSystem fs, Path path) throws IOException {
    System.out.println(path.toString());
    try (WAL.Reader reader = factory.createReader(fs, path)) {
      WAL.Entry entry = new WAL.Entry();
      while((entry = reader.next(entry)) != null) {
        System.out.println(reader.getPosition() + " " + entry.getKey().toString() +
            " " + entry.getEdit().estimatedSerializedSizeOf());
      }
    }
  }

  public static void main(String [] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: WALReader <FILE> [<FILE>...]");
      return;
    }
    Configuration configuration = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(configuration);
    WALFactory factory = null;
    try {
      factory = WALFactory.getInstance(configuration);
      for (String p: args) {
        read(factory, fs, new Path(p));
      }
    } finally {
      if (factory != null) {
        factory.close();
      }
    }
  }
}
