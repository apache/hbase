/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;

/**
 * Fixes an inefficiency in Hadoop's Gzip codec, allowing to reuse compression
 * streams.
 */
public class ReusableStreamGzipCodec extends GzipCodec {

  private static final Log LOG = LogFactory.getLog(Compression.class);

  /**
   * A bridge that wraps around a DeflaterOutputStream to make it a
   * CompressionOutputStream.
   */
  protected static class ReusableGzipOutputStream extends CompressorStream {

    private static final int GZIP_HEADER_LENGTH = 10;

    /**
     * Fixed ten-byte gzip header. See {@link GZIPOutputStream}'s source for
     * details.
     */
    private static final byte[] GZIP_HEADER;

    static {
      // Capture the fixed ten-byte header hard-coded in GZIPOutputStream.
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] header = null;
      GZIPOutputStream gzipStream = null;
      try {
        gzipStream  = new GZIPOutputStream(baos);
        gzipStream.finish();
        header = Arrays.copyOfRange(baos.toByteArray(), 0, GZIP_HEADER_LENGTH);
      } catch (IOException e) {
        throw new RuntimeException("Could not create gzip stream", e);
      } finally {
        if (gzipStream != null) {
          try {
            gzipStream.close();
          } catch (IOException e) {
            LOG.error(e);
          }
        }
      }
      GZIP_HEADER = header;
    }

    private static class ResetableGZIPOutputStream extends GZIPOutputStream {
      public ResetableGZIPOutputStream(OutputStream out) throws IOException {
        super(out);
      }

      public void resetState() throws IOException {
        def.reset();
        crc.reset();
        out.write(GZIP_HEADER);
      }
    }

    public ReusableGzipOutputStream(OutputStream out) throws IOException {
      super(new ResetableGZIPOutputStream(out));
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] data, int offset, int length) throws IOException {
      out.write(data, offset, length);
    }

    @Override
    public void finish() throws IOException {
      ((GZIPOutputStream) out).finish();
    }

    @Override
    public void resetState() throws IOException {
      ((ResetableGZIPOutputStream) out).resetState();
    }
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    if (ZlibFactory.isNativeZlibLoaded(getConf())) {
      return super.createOutputStream(out);
    }
    return new ReusableGzipOutputStream(out);
  }

}
