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
package org.apache.hadoop.hbase.codec;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.CellOutputStream;

/**
 * Encoder/Decoder for Cell.
 *
 * <p>Like {@link org.apache.hadoop.hbase.io.encoding.DataBlockEncoder} 
 * only Cell-based rather than KeyValue version 1 based and without presuming 
 * an hfile context.  Intent is an Interface that will work for hfile and rpc.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
public interface Codec {
  // TODO: interfacing with {@link org.apache.hadoop.hbase.io.encoding.DataBlockEncoder}
  /**
   * Call flush when done.  Some encoders may not put anything on the stream until flush is called.
   * On flush, let go of any resources used by the encoder.
   */
  interface Encoder extends CellOutputStream {}

  /**
   * Implementations should implicitly clean up any resources allocated when the
   * Decoder/CellScanner runs off the end of the cell block. Do this rather than require the user
   * call close explicitly.
   */
  interface Decoder extends CellScanner {};

  Decoder getDecoder(InputStream is);
  Encoder getEncoder(OutputStream os);
}
