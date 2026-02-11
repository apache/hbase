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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.io.DelegatingInputStream;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALTailingReader;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * A WAL reader for replication. It supports reset so can be used to tail a WAL file which is being
 * written currently.
 */
@InterfaceAudience.Private
public class ProtobufWALTailingReader extends AbstractProtobufWALReader
  implements WALTailingReader {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufWALTailingReader.class);

  private DelegatingInputStream delegatingInput;

  private Entry pendingEntry = null;
  private int pendingRemainingCells = 0;
  private long pendingResumePosition = -1;

  private static final class ReadWALKeyResult {
    final State state;
    final Entry entry;
    final int followingKvCount;

    public ReadWALKeyResult(State state, Entry entry, int followingKvCount) {
      this.state = state;
      this.entry = entry;
      this.followingKvCount = followingKvCount;
    }
  }

  private static final ReadWALKeyResult KEY_ERROR_AND_RESET =
    new ReadWALKeyResult(State.ERROR_AND_RESET, null, 0);

  private static final ReadWALKeyResult KEY_EOF_AND_RESET =
    new ReadWALKeyResult(State.EOF_AND_RESET, null, 0);

  private IOException unwrapIPBE(IOException e) {
    if (e instanceof InvalidProtocolBufferException) {
      return ((InvalidProtocolBufferException) e).unwrapIOException();
    } else {
      return e;
    }
  }

  private ReadWALKeyResult readWALKey(long originalPosition) {
    int firstByte;
    try {
      firstByte = delegatingInput.read();
    } catch (IOException e) {
      LOG.warn("Failed to read wal key length first byte", e);
      return KEY_ERROR_AND_RESET;
    }
    if (firstByte == -1) {
      return KEY_EOF_AND_RESET;
    }
    int size;
    try {
      size = CodedInputStream.readRawVarint32(firstByte, delegatingInput);
    } catch (IOException e) {
      // if we are reading a partial WALTrailer, the size will just be 0 so we will not get an
      // exception here, so do not need to check whether it is a partial WALTrailer.
      if (
        e instanceof InvalidProtocolBufferException
          && ProtobufUtil.isEOF((InvalidProtocolBufferException) e)
      ) {
        LOG.info("EOF while reading WALKey, originalPosition={}, currentPosition={}, error={}",
          originalPosition, getPositionQuietly(), e.toString());
        return KEY_EOF_AND_RESET;
      } else {
        LOG.warn("Failed to read wal key length", e);
        return KEY_ERROR_AND_RESET;
      }
    }
    if (size < 0) {
      LOG.warn("Negative pb message size read: {}, malformed WAL file?", size);
      return KEY_ERROR_AND_RESET;
    }
    int available;
    try {
      available = delegatingInput.available();
    } catch (IOException e) {
      LOG.warn("Failed to get available bytes", e);
      return KEY_ERROR_AND_RESET;
    }
    if (available > 0 && available < size) {
      LOG.info("Available stream not enough for edit, available={}, entry size={} at offset={}",
        available, size, getPositionQuietly());
      return KEY_EOF_AND_RESET;
    }
    WALProtos.WALKey walKey;
    try {
      if (available > 0) {
        walKey = WALProtos.WALKey.parseFrom(ByteStreams.limit(delegatingInput, size));
      } else {
        byte[] content = new byte[size];
        ByteStreams.readFully(delegatingInput, content);
        walKey = WALProtos.WALKey.parseFrom(content);
      }
    } catch (IOException e) {
      e = unwrapIPBE(e);
      if (
        e instanceof EOFException || (e instanceof InvalidProtocolBufferException
          && ProtobufUtil.isEOF((InvalidProtocolBufferException) e))
      ) {
        LOG.info("EOF while reading WALKey, originalPosition={}, currentPosition={}, error={}",
          originalPosition, getPositionQuietly(), e.toString());
        return KEY_EOF_AND_RESET;
      } else {
        boolean isWALTrailer;
        try {
          isWALTrailer = isWALTrailer(originalPosition);
        } catch (IOException ioe) {
          LOG.warn("Error while testing whether this is a partial WAL trailer, originalPosition={},"
            + " currentPosition={}", originalPosition, getPositionQuietly(), e);
          return KEY_ERROR_AND_RESET;
        }
        if (isWALTrailer) {
          LOG.info("Reached partial WAL Trailer(EOF) while reading WALKey, originalPosition={},"
            + " currentPosition={}", originalPosition, getPositionQuietly(), e);
          return KEY_EOF_AND_RESET;
        } else {
          // for all other type of IPBEs or IOEs, it means the WAL key is broken
          LOG.warn("Error while reading WALKey, originalPosition={}, currentPosition={}",
            originalPosition, getPositionQuietly(), e);
          return KEY_ERROR_AND_RESET;
        }
      }
    }
    Entry entry = new Entry();
    try {
      entry.getKey().readFieldsFromPb(walKey, byteStringUncompressor);
    } catch (IOException e) {
      LOG.warn("Failed to read wal key fields from pb message", e);
      return KEY_ERROR_AND_RESET;
    }
    return new ReadWALKeyResult(State.NORMAL, entry,
      walKey.hasFollowingKvCount() ? walKey.getFollowingKvCount() : 0);
  }

  private Result editEof() {
    return hasCompression
      ? State.EOF_AND_RESET_COMPRESSION.getResult()
      : State.EOF_AND_RESET.getResult();
  }

  private Result editError() {
    return hasCompression
      ? State.ERROR_AND_RESET_COMPRESSION.getResult()
      : State.ERROR_AND_RESET.getResult();
  }

  private Result readWALEdit(Entry entry, int followingKvCount) {
    return readCellsIntoEntry(entry, followingKvCount, false);
  }

  private Result readCellsIntoEntry(Entry entry, int remainingCells, boolean isResume) {
    long posBefore;
    try {
      posBefore = inputStream.getPos();
    } catch (IOException e) {
      LOG.warn("failed to get position", e);
      return State.ERROR_AND_RESET.getResult();
    }
    if (remainingCells == 0) {
      if (!isResume) {
        LOG.trace("WALKey has no KVs that follow it; trying the next one. current offset={}",
          posBefore);
      }
      return new Result(State.NORMAL, entry, posBefore);
    }
    long lastGoodPos = posBefore;
    int cellsRead = 0;
    for (int i = 0; i < remainingCells; i++) {
      try {
        lastGoodPos = inputStream.getPos();
      } catch (IOException e) {
        LOG.warn("failed to get position before cell read", e);
        return editError();
      }
      boolean advanced;
      try {
        advanced = cellDecoder.advance();
      } catch (Exception e) {
        IOException realEofEx = extractHiddenEof(e);
        if (realEofEx != null) {
          LOG.debug("EOF after reading {} of {} cells; started reading at {}, last good pos={}",
            cellsRead, remainingCells, posBefore, lastGoodPos, realEofEx);
          return savePendingAndReturnEof(entry, remainingCells - cellsRead, lastGoodPos);
        } else {
          LOG.warn("Error after reading {} of {} cells; started reading at {}, read up to {}",
            cellsRead, remainingCells, posBefore, getPositionQuietly(), e);
          return editError();
        }
      }
      if (!advanced) {
        LOG.debug("EOF (advance returned false) after reading {} of {} cells; started at {},"
          + " last good pos={}", cellsRead, remainingCells, posBefore, lastGoodPos);
        return savePendingAndReturnEof(entry, remainingCells - cellsRead, lastGoodPos);
      }
      entry.getEdit().add(cellDecoder.current());
      cellsRead++;
    }
    long posAfter;
    try {
      posAfter = inputStream.getPos();
    } catch (IOException e) {
      LOG.warn("failed to get position", e);
      return editError();
    }
    if (trailerPresent && posAfter > this.walEditsStopOffset) {
      LOG.error("Read WALTrailer while reading WALEdits. wal: {}, inputStream.getPos(): {},"
        + " walEditsStopOffset: {}", path, posAfter, walEditsStopOffset);
      return editEof();
    }
    return new Result(State.NORMAL, entry, posAfter);
  }

  private Result savePendingAndReturnEof(Entry entry, int remaining, long resumePos) {
    if (hasCompression) {
      pendingEntry = entry;
      pendingRemainingCells = remaining;
      pendingResumePosition = resumePos;
      return new Result(State.EOF_AND_RESET, null, resumePos);
    }
    return editEof();
  }

  private void clearPendingState() {
    pendingEntry = null;
    pendingRemainingCells = 0;
    pendingResumePosition = -1;
  }

  @Override
  public Result next(long limit) {
    if (pendingEntry != null) {
      long originalPosition;
      try {
        originalPosition = inputStream.getPos();
      } catch (IOException e) {
        LOG.warn("failed to get position", e);
        clearPendingState();
        return State.EOF_AND_RESET.getResult();
      }
      if (limit < 0) {
        delegatingInput.setDelegate(inputStream);
      } else if (limit <= originalPosition) {
        return State.EOF_AND_RESET.getResult();
      } else {
        delegatingInput.setDelegate(ByteStreams.limit(inputStream, limit - originalPosition));
      }
      Entry entry = pendingEntry;
      int remaining = pendingRemainingCells;
      clearPendingState();
      return readCellsIntoEntry(entry, remaining, true);
    }
    long originalPosition;
    try {
      originalPosition = inputStream.getPos();
    } catch (IOException e) {
      LOG.warn("failed to get position", e);
      return State.EOF_AND_RESET.getResult();
    }
    if (reachWALEditsStopOffset(originalPosition)) {
      return State.EOF_WITH_TRAILER.getResult();
    }
    if (limit < 0) {
      // should be closed WAL file, set to no limit, i.e, just use the original inputStream
      delegatingInput.setDelegate(inputStream);
    } else if (limit <= originalPosition) {
      // no data available, just return EOF
      return State.EOF_AND_RESET.getResult();
    } else {
      // calculate the remaining bytes we can read and set
      delegatingInput.setDelegate(ByteStreams.limit(inputStream, limit - originalPosition));
    }
    ReadWALKeyResult readKeyResult = readWALKey(originalPosition);
    if (readKeyResult.state != State.NORMAL) {
      return readKeyResult.state.getResult();
    }
    return readWALEdit(readKeyResult.entry, readKeyResult.followingKvCount);
  }

  private void skipHeader(FSDataInputStream stream) throws IOException {
    stream.seek(PB_WAL_MAGIC.length);
    int headerLength = StreamUtils.readRawVarint32(stream);
    stream.seek(stream.getPos() + headerLength);
  }

  @Override
  public void resetTo(long position, boolean resetCompression) throws IOException {
    if (resetCompression) {
      clearPendingState();
    }
    long seekPosition = position;
    if (!resetCompression && pendingResumePosition > 0) {
      seekPosition = pendingResumePosition;
    }
    close();
    Pair<FSDataInputStream, FileStatus> pair = open();
    boolean resetSucceed = false;
    try {
      if (!trailerPresent) {
        // try read trailer this time
        readTrailer(pair.getFirst(), pair.getSecond());
      }
      inputStream = pair.getFirst();
      delegatingInput.setDelegate(inputStream);
      if (position < 0) {
        // read from the beginning
        if (compressionCtx != null) {
          compressionCtx.clear();
        }
        clearPendingState();
        skipHeader(inputStream);
      } else if (resetCompression && compressionCtx != null) {
        // clear compressCtx and skip to the expected position, to fill up the dictionary
        compressionCtx.clear();
        skipHeader(inputStream);
        if (position != inputStream.getPos()) {
          skipTo(position);
        }
      } else {
        // just seek to the expected position
        inputStream.seek(seekPosition);
      }
      resetSucceed = true;
    } finally {
      if (!resetSucceed) {
        // close the input stream to avoid resource leak
        close();
      }
    }
  }

  @Override
  protected InputStream getCellCodecInputStream(FSDataInputStream stream) {
    delegatingInput = new DelegatingInputStream(stream);
    return delegatingInput;
  }

  @Override
  protected void skipTo(long position) throws IOException {
    for (;;) {
      Result result = next(-1);
      if (result.getState() != State.NORMAL) {
        throw new IOException("Can not skip to the given position " + position + ", stopped at "
          + result.getEntryEndPos() + " which is still before the give position");
      }
      if (result.getEntryEndPos() == position) {
        return;
      }
      if (result.getEntryEndPos() > position) {
        throw new IOException("Can not skip to the given position " + position + ", stopped at "
          + result.getEntryEndPos() + " which is already beyond the give position, malformed WAL?");
      }
    }
  }
}
