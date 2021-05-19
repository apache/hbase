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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * An input stream that delegates all operations to another input stream.
 * The delegate can be switched out for another at any time but to minimize the
 * possibility of violating the InputStream contract it would be best to replace
 * the delegate only once it has been fully consumed. <p> For example, a
 * ByteArrayInputStream, which is implicitly bounded by the size of the underlying
 * byte array can be converted into an unbounded stream fed by multiple instances
 * of ByteArrayInputStream, switched out one for the other in sequence.
 * <p>
 * Although multithreaded access is allowed, users of this class will want to take
 * care to order operations on this stream and the swap out of one delegate for
 * another in a way that provides a valid view of stream contents.
 */
@InterfaceAudience.Private
public class DelegatingInputStream extends InputStream {

  final AtomicReference<InputStream> lowerStream = new AtomicReference<>();

  public DelegatingInputStream(InputStream lowerStream) {
    this.lowerStream.set(lowerStream);
  }

  public InputStream getDelegate() {
    return lowerStream.get();
  }

  public void setDelegate(InputStream lowerStream) {
    this.lowerStream.set(lowerStream);
  }

  @Override
  public int read() throws IOException {
    return lowerStream.get().read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return lowerStream.get().read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return lowerStream.get().read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return lowerStream.get().skip(n);
  }

  @Override
  public int available() throws IOException {
    return lowerStream.get().available();
  }

  @Override
  public void close() throws IOException {
    lowerStream.get().close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    lowerStream.get().mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    lowerStream.get().reset();
  }

  @Override
  public boolean markSupported() {
    return lowerStream.get().markSupported();
  }

}
