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

package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * An InputStream that wraps a DataInput.
 * @see DataOutputOutputStream
 */
@InterfaceAudience.Private
public class DataInputInputStream extends InputStream {

  private DataInput in;

  /**
   * Construct an InputStream from the given DataInput. If 'in'
   * is already an InputStream, simply returns it. Otherwise, wraps
   * it in an InputStream.
   * @param in the DataInput to wrap
   * @return an InputStream instance that reads from 'in'
   */
  public static InputStream constructInputStream(DataInput in) {
    if (in instanceof InputStream) {
      return (InputStream)in;
    } else {
      return new DataInputInputStream(in);
    }
  }


  public DataInputInputStream(DataInput in) {
    this.in = in;
  }

  @Override
  public int read() throws IOException {
    return in.readUnsignedByte();
  }
}
