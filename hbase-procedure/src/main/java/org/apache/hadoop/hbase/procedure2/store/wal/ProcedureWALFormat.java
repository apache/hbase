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

package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureLoader;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.procedure2.util.ByteSlot;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALEntry;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALHeader;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALTrailer;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Helper class that contains the WAL serialization utils.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ProcedureWALFormat {
  static final byte LOG_TYPE_STREAM = 0;
  static final byte LOG_TYPE_COMPACTED = 1;
  static final byte LOG_TYPE_MAX_VALID = 1;

  static final byte HEADER_VERSION = 1;
  static final byte TRAILER_VERSION = 1;
  static final long HEADER_MAGIC = 0x31764c4157637250L;
  static final long TRAILER_MAGIC = 0x50726357414c7631L;

  @InterfaceAudience.Private
  public static class InvalidWALDataException extends IOException {
    public InvalidWALDataException(String s) {
      super(s);
    }

    public InvalidWALDataException(Throwable t) {
      super(t);
    }
  }

  interface Loader extends ProcedureLoader {
    void markCorruptedWAL(ProcedureWALFile log, IOException e);
  }

  private ProcedureWALFormat() {}

  public static void load(final Iterator<ProcedureWALFile> logs,
      final ProcedureStoreTracker tracker, final Loader loader) throws IOException {
    ProcedureWALFormatReader reader = new ProcedureWALFormatReader(tracker);
    tracker.setKeepDeletes(true);
    try {
      while (logs.hasNext()) {
        ProcedureWALFile log = logs.next();
        log.open();
        try {
          reader.read(log, loader);
        } finally {
          log.close();
        }
      }
      reader.finalize(loader);
      // The tracker is now updated with all the procedures read from the logs
      tracker.setPartialFlag(false);
      tracker.resetUpdates();
    } finally {
      tracker.setKeepDeletes(false);
    }
  }

  public static void writeHeader(OutputStream stream, ProcedureWALHeader header)
      throws IOException {
    header.writeDelimitedTo(stream);
  }

  /*
   * +-----------------+
   * | END OF WAL DATA | <---+
   * +-----------------+     |
   * |                 |     |
   * |     Tracker     |     |
   * |                 |     |
   * +-----------------+     |
   * |     version     |     |
   * +-----------------+     |
   * |  TRAILER_MAGIC  |     |
   * +-----------------+     |
   * |      offset     |-----+
   * +-----------------+
   */
  public static long writeTrailer(FSDataOutputStream stream, ProcedureStoreTracker tracker)
      throws IOException {
    long offset = stream.getPos();

    // Write EOF Entry
    ProcedureWALEntry.newBuilder()
      .setType(ProcedureWALEntry.Type.PROCEDURE_WAL_EOF)
      .build().writeDelimitedTo(stream);

    // Write Tracker
    tracker.writeTo(stream);

    stream.write(TRAILER_VERSION);
    StreamUtils.writeLong(stream, TRAILER_MAGIC);
    StreamUtils.writeLong(stream, offset);
    return stream.getPos() - offset;
  }

  public static ProcedureWALHeader readHeader(InputStream stream)
      throws IOException {
    ProcedureWALHeader header;
    try {
      header = ProcedureWALHeader.parseDelimitedFrom(stream);
    } catch (InvalidProtocolBufferException e) {
      throw new InvalidWALDataException(e);
    }

    if (header == null) {
      throw new InvalidWALDataException("No data available to read the Header");
    }

    if (header.getVersion() < 0 || header.getVersion() != HEADER_VERSION) {
      throw new InvalidWALDataException("Invalid Header version. got " + header.getVersion() +
          " expected " + HEADER_VERSION);
    }

    if (header.getType() < 0 || header.getType() > LOG_TYPE_MAX_VALID) {
      throw new InvalidWALDataException("Invalid header type. got " + header.getType());
    }

    return header;
  }

  public static ProcedureWALTrailer readTrailer(FSDataInputStream stream, long startPos, long size)
      throws IOException {
    // Beginning of the Trailer Jump. 17 = 1 byte version + 8 byte magic + 8 byte offset
    long trailerPos = size - 17;

    if (trailerPos < startPos) {
      throw new InvalidWALDataException("Missing trailer: size=" + size + " startPos=" + startPos);
    }

    stream.seek(trailerPos);
    int version = stream.read();
    if (version != TRAILER_VERSION) {
      throw new InvalidWALDataException("Invalid Trailer version. got " + version +
          " expected " + TRAILER_VERSION);
    }

    long magic = StreamUtils.readLong(stream);
    if (magic != TRAILER_MAGIC) {
      throw new InvalidWALDataException("Invalid Trailer magic. got " + magic +
          " expected " + TRAILER_MAGIC);
    }

    long trailerOffset = StreamUtils.readLong(stream);
    stream.seek(trailerOffset);

    ProcedureWALEntry entry = readEntry(stream);
    if (entry.getType() != ProcedureWALEntry.Type.PROCEDURE_WAL_EOF) {
      throw new InvalidWALDataException("Invalid Trailer begin");
    }

    ProcedureWALTrailer trailer = ProcedureWALTrailer.newBuilder()
      .setVersion(version)
      .setTrackerPos(stream.getPos())
      .build();
    return trailer;
  }

  public static ProcedureWALEntry readEntry(InputStream stream) throws IOException {
    return ProcedureWALEntry.parseDelimitedFrom(stream);
  }

  public static void writeEntry(ByteSlot slot, ProcedureWALEntry.Type type,
      Procedure proc, Procedure[] subprocs) throws IOException {
    ProcedureWALEntry.Builder builder = ProcedureWALEntry.newBuilder();
    builder.setType(type);
    builder.addProcedure(Procedure.convert(proc));
    if (subprocs != null) {
      for (int i = 0; i < subprocs.length; ++i) {
        builder.addProcedure(Procedure.convert(subprocs[i]));
      }
    }
    builder.build().writeDelimitedTo(slot);
  }

  public static void writeInsert(ByteSlot slot, Procedure proc)
      throws IOException {
    writeEntry(slot, ProcedureWALEntry.Type.PROCEDURE_WAL_INIT, proc, null);
  }

  public static void writeInsert(ByteSlot slot, Procedure proc, Procedure[] subprocs)
      throws IOException {
    writeEntry(slot, ProcedureWALEntry.Type.PROCEDURE_WAL_INSERT, proc, subprocs);
  }

  public static void writeUpdate(ByteSlot slot, Procedure proc)
      throws IOException {
    writeEntry(slot, ProcedureWALEntry.Type.PROCEDURE_WAL_UPDATE, proc, null);
  }

  public static void writeDelete(ByteSlot slot, long procId)
      throws IOException {
    ProcedureWALEntry.Builder builder = ProcedureWALEntry.newBuilder();
    builder.setType(ProcedureWALEntry.Type.PROCEDURE_WAL_DELETE);
    builder.setProcId(procId);
    builder.build().writeDelimitedTo(slot);
  }
}