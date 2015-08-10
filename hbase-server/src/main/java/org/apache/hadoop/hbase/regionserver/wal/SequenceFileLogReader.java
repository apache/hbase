/**
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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.FilterInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;

@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX, HBaseInterfaceAudience.CONFIG})
public class SequenceFileLogReader extends ReaderBase {
  private static final Log LOG = LogFactory.getLog(SequenceFileLogReader.class);

  // Legacy stuff from pre-PB WAL metadata.
  private static final Text WAL_VERSION_KEY = new Text("version");
  // Let the version be 1.  Let absence of a version meta tag be old, version 0.
  // Set this version '1' to be the version that introduces compression,
  // the COMPRESSION_VERSION.
  private static final int COMPRESSION_VERSION = 1;
  private static final Text WAL_COMPRESSION_TYPE_KEY = new Text("compression.type");
  private static final Text DICTIONARY_COMPRESSION_TYPE = new Text("dictionary");

  /**
   * Hack just to set the correct file length up in SequenceFile.Reader.
   * See HADOOP-6307.  The below is all about setting the right length on the
   * file we are reading.  fs.getFileStatus(file).getLen() is passed down to
   * a private SequenceFile.Reader constructor.  This won't work.  Need to do
   * the available on the stream.  The below is ugly.  It makes getPos, the
   * first time its called, return length of the file -- i.e. tell a lie -- just
   * so this line up in SF.Reader's constructor ends up with right answer:
   *
   *         this.end = in.getPos() + length;
   *
   */
  private static class WALReader extends SequenceFile.Reader {

    WALReader(final FileSystem fs, final Path p, final Configuration c)
    throws IOException {
      super(fs, p, c);
    }

    @Override
    protected FSDataInputStream openFile(FileSystem fs, Path file,
      int bufferSize, long length)
    throws IOException {
      return new WALReaderFSDataInputStream(super.openFile(fs, file,
        bufferSize, length), length);
    }

    /**
     * Override just so can intercept first call to getPos.
     */
    static class WALReaderFSDataInputStream extends FSDataInputStream {
      private boolean firstGetPosInvocation = true;
      private long length;

      WALReaderFSDataInputStream(final FSDataInputStream is, final long l)
      throws IOException {
        super(is);
        this.length = l;
      }

      // This section can be confusing.  It is specific to how HDFS works.
      // Let me try to break it down.  This is the problem:
      //
      //  1. HDFS DataNodes update the NameNode about a filename's length
      //     on block boundaries or when a file is closed. Therefore,
      //     if an RS dies, then the NN's fs.getLength() can be out of date
      //  2. this.in.available() would work, but it returns int &
      //     therefore breaks for files > 2GB (happens on big clusters)
      //  3. DFSInputStream.getFileLength() gets the actual length from the DNs
      //  4. DFSInputStream is wrapped 2 levels deep : this.in.in
      //
      // So, here we adjust getPos() using getFileLength() so the
      // SequenceFile.Reader constructor (aka: first invocation) comes out
      // with the correct end of the file:
      //         this.end = in.getPos() + length;
      @Override
      public long getPos() throws IOException {
        if (this.firstGetPosInvocation) {
          this.firstGetPosInvocation = false;
          long adjust = 0;

          try {
            Field fIn = FilterInputStream.class.getDeclaredField("in");
            fIn.setAccessible(true);
            Object realIn = fIn.get(this.in);
            if (this.in.getClass().getName().endsWith("HdfsDataInputStream")
                || realIn.getClass().getName().endsWith("DFSInputStream")) {
              // Here we try to use reflection because HdfsDataInputStream is not available in
              // hadoop 1.1. HBASE-5878
              try {
                Class<?> hdfsDataInputStream =
                    Class.forName("org.apache.hadoop.hdfs.client.HdfsDataInputStream");
                Method getVisibleLength = hdfsDataInputStream.getDeclaredMethod("getVisibleLength");
                getVisibleLength.setAccessible(true);
                long realLength =
                    ((Long) getVisibleLength.invoke(realIn, new Object[] {})).longValue();
                assert (realLength >= this.length);
                adjust = realLength - this.length;
              } catch (ClassNotFoundException e) {
                // Failed to found the class HdfsDataInputStream, may be it is deployed on hadoop
                // 1.1
                // In hadoop 0.22, DFSInputStream is a standalone class. Before this,
                // it was an inner class of DFSClient.
                Method getFileLength =
                    realIn.getClass().getDeclaredMethod("getFileLength", new Class<?>[] {});
                getFileLength.setAccessible(true);
                long realLength =
                    ((Long) getFileLength.invoke(realIn, new Object[] {})).longValue();
                assert (realLength >= this.length);
                adjust = realLength - this.length;
              }
            } else {
              LOG.info("Input stream class: " + realIn.getClass().getName()
                  + ", not adjusting length");
            }
          } catch (Exception e) {
            LOG.warn("Error while trying to get accurate file length.  "
                + "Truncation / data loss may occur if RegionServers die.", e);
            throw new IOException(e);
          }

          return adjust + super.getPos();
        }
        return super.getPos();
      }
    }
  }

  // Protected for tests.
  protected SequenceFile.Reader reader;
  long entryStart = 0; // needed for logging exceptions

  public SequenceFileLogReader() {
    super();
  }

  @Override
  public void close() throws IOException {
    try {
      if (reader != null) {
        this.reader.close();
        this.reader = null;
      }
    } catch (IOException ioe) {
      throw addFileInfoToException(ioe);
    }
  }

  @Override
  public long getPosition() throws IOException {
    return reader != null ? reader.getPosition() : 0;
  }

  @Override
  public void reset() throws IOException {
    // Resetting the reader lets us see newly added data if the file is being written to
    // We also keep the same compressionContext which was previously populated for this file
    reader = new WALReader(fs, path, conf);
  }

  @Override
  protected String initReader(FSDataInputStream stream) throws IOException {
    // We don't use the stream because we have to have the magic stream above.
    if (stream != null) {
      stream.close();
    }
    reset();
    return null;
  }
  
  @Override
  protected void initAfterCompression(String cellCodecClsName) throws IOException {
    // Nothing to do here
  }

  @Override
  protected void initAfterCompression() throws IOException {
    // Nothing to do here
  }

  @Override
  protected boolean hasCompression() {
    return isWALCompressionEnabled(reader.getMetadata());
  }

  @Override
  protected boolean hasTagCompression() {
    // Tag compression not supported with old SequenceFileLog Reader/Writer
    return false;
  }

  /**
   * Call this method after init() has been executed
   * @return whether WAL compression is enabled
   */
  static boolean isWALCompressionEnabled(final Metadata metadata) {
    // Check version is >= VERSION?
    Text txt = metadata.get(WAL_VERSION_KEY);
    if (txt == null || Integer.parseInt(txt.toString()) < COMPRESSION_VERSION) {
      return false;
    }
    // Now check that compression type is present.  Currently only one value.
    txt = metadata.get(WAL_COMPRESSION_TYPE_KEY);
    return txt != null && txt.equals(DICTIONARY_COMPRESSION_TYPE);
  }


  @Override
  protected boolean readNext(Entry e) throws IOException {
    try {
      boolean hasNext = this.reader.next(e.getKey(), e.getEdit());
      if (!hasNext) return false;
      // Scopes are probably in WAL edit, move to key
      NavigableMap<byte[], Integer> scopes = e.getEdit().getAndRemoveScopes();
      if (scopes != null) {
        e.getKey().readOlderScopes(scopes);
      }
      return true;
    } catch (IOException ioe) {
      throw addFileInfoToException(ioe);
    }
  }

  @Override
  protected void seekOnFs(long pos) throws IOException {
    try {
      reader.seek(pos);
    } catch (IOException ioe) {
      throw addFileInfoToException(ioe);
    }
  }

  protected IOException addFileInfoToException(final IOException ioe)
  throws IOException {
    long pos = -1;
    try {
      pos = getPosition();
    } catch (IOException e) {
      LOG.warn("Failed getting position to add to throw", e);
    }

    // See what SequenceFile.Reader thinks is the end of the file
    long end = Long.MAX_VALUE;
    try {
      Field fEnd = SequenceFile.Reader.class.getDeclaredField("end");
      fEnd.setAccessible(true);
      end = fEnd.getLong(this.reader);
    } catch(NoSuchFieldException nfe) {
       /* reflection failure, keep going */
    } catch(IllegalAccessException iae) {
       /* reflection failure, keep going */
    } catch(Exception e) {
       /* All other cases. Should we handle it more aggressively? */
       LOG.warn("Unexpected exception when accessing the end field", e);
    }
 
    String msg = (this.path == null? "": this.path.toString()) +
      ", entryStart=" + entryStart + ", pos=" + pos +
      ((end == Long.MAX_VALUE) ? "" : ", end=" + end) +
      ", edit=" + this.edit;

    // Enhance via reflection so we don't change the original class type
    try {
      return (IOException) ioe.getClass()
        .getConstructor(String.class)
        .newInstance(msg)
        .initCause(ioe);
    } catch(NoSuchMethodException nfe) {
       /* reflection failure, keep going */
    } catch(IllegalAccessException iae) {
       /* reflection failure, keep going */
    } catch(Exception e) {
       /* All other cases. Should we handle it more aggressively? */
       LOG.warn("Unexpected exception when accessing the end field", e);
    }
    return ioe;
  }
}
