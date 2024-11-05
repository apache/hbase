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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.EnumSet;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.DataChecksum;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A dummy DFSOutputStream which is mainly used for lease renewal.
 * <p>
 * We have to put it under this package as we want to override a package private method.
 */
@InterfaceAudience.Private
public final class DummyDFSOutputStream extends DFSOutputStream {

  private final AsyncFSOutput delegate;

  public DummyDFSOutputStream(AsyncFSOutput output, DFSClient dfsClient, String src,
    HdfsFileStatus stat, EnumSet<CreateFlag> flag, DataChecksum checksum) {
    super(dfsClient, src, stat, flag, null, checksum, null, false);
    this.delegate = output;
  }

  // public for testing
  @Override
  public void abort() throws IOException {
    delegate.close();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
