/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/**
 * IO engine that stores data to a file on the local file system.
 */
@InterfaceAudience.Private
public class FileIOEngine implements PersistentIOEngine {
  private static final Log LOG = LogFactory.getLog(FileIOEngine.class);
  public static final String FILE_DELIMITER = ",";
  private static final DuFileCommand du = new DuFileCommand(new String[] {"du", ""});

  private final String[] filePaths;
  private final FileChannel[] fileChannels;
  private final RandomAccessFile[] rafs;
  private final ReentrantLock[] channelLocks;

  private final long sizePerFile;
  private final long capacity;
  private final String algorithmName;
  private boolean oldVersion;

  private FileReadAccessor readAccessor = new FileReadAccessor();
  private FileWriteAccessor writeAccessor = new FileWriteAccessor();

  public FileIOEngine(String algorithmName, String persistentPath,
    long capacity, String... filePaths) throws IOException {
    this.sizePerFile = capacity / filePaths.length;
    this.capacity = this.sizePerFile * filePaths.length;
    this.filePaths = filePaths;
    this.fileChannels = new FileChannel[filePaths.length];
    this.rafs = new RandomAccessFile[filePaths.length];
    this.channelLocks = new ReentrantLock[filePaths.length];
    this.algorithmName = algorithmName;
    verifyFileIntegrity(persistentPath);
    init();
  }

  /**
   * Verify cache files's integrity
   * @param persistentPath the backingMap persistent path
   */
  @Override
  public void verifyFileIntegrity(String persistentPath) {
    if (persistentPath != null) {
      byte[] persistentChecksum = readPersistentChecksum(persistentPath);
      if (!oldVersion) {
        try {
          byte[] calculateChecksum = calculateChecksum();
          if (!Bytes.equals(persistentChecksum, calculateChecksum)) {
            LOG.warn("The persistent checksum is " + Bytes.toString(persistentChecksum) +
              ", but the calculate checksum is " + Bytes.toString(calculateChecksum));
            throw new IOException();
          }
        } catch (IOException ioex) {
          LOG.error("File verification failed because of ", ioex);
          // delete cache files and backingMap persistent file.
          deleteCacheDataFile();
          new File(persistentPath).delete();
        } catch (NoSuchAlgorithmException nsae) {
          LOG.error("No such algorithm " + algorithmName, nsae);
          throw new RuntimeException(nsae);
        }
      }
    } else {
      // not configure persistent path
      deleteCacheDataFile();
    }
  }

  private void init() throws IOException {
    for (int i = 0; i < filePaths.length; i++) {
      String filePath = filePaths[i];
      try {
        rafs[i] = new RandomAccessFile(filePath, "rw");
        long totalSpace = new File(filePath).getTotalSpace();
        if (totalSpace < sizePerFile) {
          // The next setting length will throw exception,logging this message
          // is just used for the detail reason of exception，
          String msg = "Only " + StringUtils.byteDesc(totalSpace)
            + " total space under " + filePath + ", not enough for requested "
            + StringUtils.byteDesc(sizePerFile);
          LOG.warn(msg);
        }
        rafs[i].setLength(sizePerFile);
        fileChannels[i] = rafs[i].getChannel();
        channelLocks[i] = new ReentrantLock();
        LOG.info("Allocating cache " + StringUtils.byteDesc(sizePerFile)
          + ", on the path: " + filePath);
      } catch (IOException fex) {
        LOG.error("Failed allocating cache on " + filePath, fex);
        shutdown();
        throw fex;
      }
    }
  }

  @Override
  public String toString() {
    return "ioengine=" + this.getClass().getSimpleName() + ", paths="
        + Arrays.asList(filePaths) + ", capacity=" + String.format("%,d", this.capacity);
  }

  /**
   * File IO engine is always able to support persistent storage for the cache
   * @return true
   */
  @Override
  public boolean isPersistent() {
    return true;
  }

  /**
   * Transfers data from file to the given byte buffer
   * @param dstBuffer the given byte buffer into which bytes are to be written
   * @param offset The offset in the file where the first byte to be read
   * @return number of bytes read
   * @throws IOException
   */
  @Override
  public int read(ByteBuffer dstBuffer, long offset) throws IOException {
    if (dstBuffer.remaining() != 0) {
      return accessFile(readAccessor, dstBuffer, offset);
    }
    return 0;
  }

  @VisibleForTesting
  void closeFileChannels() {
    for (FileChannel fileChannel: fileChannels) {
      try {
        fileChannel.close();
      } catch (IOException e) {
        LOG.warn("Failed to close FileChannel", e);
      }
    }
  }

  /**
   * Transfers data from the given byte buffer to file
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the file where the first byte to be written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    if (!srcBuffer.hasRemaining()) {
      return;
    }
    accessFile(writeAccessor, srcBuffer, offset);
  }

  /**
   * Sync the data to file after writing
   * @throws IOException
   */
  @Override
  public void sync() throws IOException {
    for (int i = 0; i < fileChannels.length; i++) {
      try {
        if (fileChannels[i] != null) {
          fileChannels[i].force(true);
        }
      } catch (IOException ie) {
        LOG.warn("Failed syncing data to " + this.filePaths[i]);
        throw ie;
      }
    }
  }

  /**
   * Close the file
   */
  @Override
  public void shutdown() {
    for (int i = 0; i < filePaths.length; i++) {
      try {
        if (fileChannels[i] != null) {
          fileChannels[i].close();
        }
        if (rafs[i] != null) {
          rafs[i].close();
        }
      } catch (IOException ex) {
        LOG.error("Failed closing " + filePaths[i] + " when shudown the IOEngine", ex);
      }
    }
  }

  private int accessFile(FileAccessor accessor, ByteBuffer buffer, long globalOffset)
      throws IOException {
    int startFileNum = getFileNum(globalOffset);
    int remainingAccessDataLen = buffer.remaining();
    int endFileNum = getFileNum(globalOffset + remainingAccessDataLen - 1);
    int accessFileNum = startFileNum;
    long accessOffset = getAbsoluteOffsetInFile(accessFileNum, globalOffset);
    int bufLimit = buffer.limit();
    while (true) {
      FileChannel fileChannel = fileChannels[accessFileNum];
      int accessLen = 0;
      if (endFileNum > accessFileNum) {
        // short the limit;
        buffer.limit((int) (buffer.limit() - remainingAccessDataLen + sizePerFile - accessOffset));
      }
      try {
        accessLen = accessor.access(fileChannel, buffer, accessOffset);
      } catch (ClosedByInterruptException e) {
        throw e;
      } catch (ClosedChannelException e) {
        refreshFileConnection(accessFileNum, e);
        continue;
      }
      // recover the limit
      buffer.limit(bufLimit);
      if (accessLen < remainingAccessDataLen) {
        remainingAccessDataLen -= accessLen;
        accessFileNum++;
        accessOffset = 0;
      } else {
        break;
      }
      if (accessFileNum >= fileChannels.length) {
        throw new IOException("Required data len " + StringUtils.byteDesc(buffer.remaining())
            + " exceed the engine's capacity " + StringUtils.byteDesc(capacity) + " where offset="
            + globalOffset);
      }
    }
    return bufLimit;
  }

  /**
   * Get the absolute offset in given file with the relative global offset.
   * @param fileNum
   * @param globalOffset
   * @return the absolute offset
   */
  private long getAbsoluteOffsetInFile(int fileNum, long globalOffset) {
    return globalOffset - fileNum * sizePerFile;
  }

  private int getFileNum(long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("Unexpected offset " + offset);
    }
    int fileNum = (int) (offset / sizePerFile);
    if (fileNum >= fileChannels.length) {
      throw new RuntimeException("Not expected offset " + offset + " where capacity=" + capacity);
    }
    return fileNum;
  }

  @VisibleForTesting
  FileChannel[] getFileChannels() {
    return fileChannels;
  }

  @VisibleForTesting
  void refreshFileConnection(int accessFileNum, IOException ioe) throws IOException {
    ReentrantLock channelLock = channelLocks[accessFileNum];
    channelLock.lock();
    try {
      FileChannel fileChannel = fileChannels[accessFileNum];
      if (fileChannel != null) {
        // Don't re-open a channel if we were waiting on another
        // thread to re-open the channel and it is now open.
        if (fileChannel.isOpen()) {
          return;
        }
        fileChannel.close();
      }
      LOG.warn("Caught ClosedChannelException accessing BucketCache, reopening file: "
          + filePaths[accessFileNum], ioe);
      rafs[accessFileNum] = new RandomAccessFile(filePaths[accessFileNum], "rw");
      fileChannels[accessFileNum] = rafs[accessFileNum].getChannel();
    } finally{
      channelLock.unlock();
    }
  }

  /**
   * Read the persistent checksum from persistent path
   * @param persistentPath the backingMap persistent path
   * @return the persistent checksum
   */
  private byte[] readPersistentChecksum(String persistentPath) {
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(persistentPath))) {
      byte[] PBMagic = new byte[ProtobufUtil.PB_MAGIC.length];
      ois.read(PBMagic);
      if (Bytes.equals(ProtobufUtil.PB_MAGIC, PBMagic)) {
        int length = ois.readInt();
        byte[] persistentChecksum = new byte[length];
        ois.read(persistentChecksum);
        return persistentChecksum;
      } else {
        // if the persistent file is not start with PB_MAGIC, it's an old version file
        oldVersion = true;
      }
    } catch (IOException ioex) {
      LOG.warn("Failed read persistent checksum, because of " + ioex);
      return null;
    }
    return null;
  }

  @Override
  public void deleteCacheDataFile() {
    if (filePaths == null) {
      return;
    }
    for (String file : filePaths) {
      new File(file).delete();
    }
  }

  @Override
  public byte[] calculateChecksum()
    throws IOException, NoSuchAlgorithmException {

    if (filePaths == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (String filePath : filePaths){
      File file = new File(filePath);
      if (file.exists()){
        sb.append(filePath);
        sb.append(getFileSize(filePath));
        sb.append(file.lastModified());
      } else {
        throw new IOException("Cache file: " + filePath + " is not exists.");
      }
    }
    MessageDigest messageDigest = MessageDigest.getInstance(algorithmName);
    messageDigest.update(Bytes.toBytes(sb.toString()));
    return messageDigest.digest();
  }

  @Override
  public boolean isOldVersion() {
    return oldVersion;
  }

  /**
   * Using Linux command du to get file's real size
   * @param filePath the file
   * @return file's real size
   * @throws IOException something happened like file not exists
   */
  private static long getFileSize(String filePath) throws IOException {
    du.setExecCommand(filePath);
    du.execute();
    return Long.parseLong(du.getOutput().split("\t")[0]);
  }

  private static class DuFileCommand extends Shell.ShellCommandExecutor {
    private String[] execCommand;

    DuFileCommand(String[] execString) {
      super(execString);
      execCommand = execString;
    }

    void setExecCommand(String filePath) {
      this.execCommand[1] = filePath;
    }

    @Override
    public String[] getExecString() {
      return this.execCommand;
    }
  }

  private static interface FileAccessor {
    int access(FileChannel fileChannel, ByteBuffer byteBuffer, long accessOffset)
        throws IOException;
  }

  private static class FileReadAccessor implements FileAccessor {
    @Override
    public int access(FileChannel fileChannel, ByteBuffer byteBuffer, long accessOffset)
        throws IOException {
      return fileChannel.read(byteBuffer, accessOffset);
    }
  }

  private static class FileWriteAccessor implements FileAccessor {
    @Override
    public int access(FileChannel fileChannel, ByteBuffer byteBuffer, long accessOffset)
        throws IOException {
      return fileChannel.write(byteBuffer, accessOffset);
    }
  }

}
