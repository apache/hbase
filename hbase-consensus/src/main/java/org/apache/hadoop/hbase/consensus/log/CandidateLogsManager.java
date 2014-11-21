/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.util.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.NoSuchElementException;

/**
 *  Manages the candidate log files for a given quorum on a member node. It holds
 *  a collection of committed transaction logs fetched from other members. These
 *  "candidate" logs can overlap in their ranges. The transaction log manager can
 *  inform the candidate log manager of its current indices so that the latter
 *  can merge some of its holdings into the committed transaction logs category.
 *
 *  <p/>
 *  The candidate log directory is of a flat structure. Each candidate log file is
 *  uniquely identified by its name.
 *
 *  <p/>
 *  Data Structures
 *
 *  We maintain two maps of LogFileInfo, one indexed by initial index and the other
 *  by filename.
 *
 *  candidateLogsByIndex --
 *
 *  candidateLogsByName --
 *
 *  Invariants
 *
 *  1. The two maps, candidateLogsByIndex and candidateLogsByName agree.
 *  2. No two LogFileInfo overlap each other.
 */
public class CandidateLogsManager {
  private static final Logger LOG = LoggerFactory.getLogger(CandidateLogsManager.class);
  private static final Logger CheckInvariantsLogger = LoggerFactory.getLogger(
      CandidateLogsManager.class.getName()+"#checkInvariants");

  /****************************************************************************
   *                             member variables
   ****************************************************************************/
  private final String contextName;
  private final String logsDirectory;
  private final String logPrefix;

  private Configuration conf;
  private volatile long minCandidateLogSize =
    HConstants.RAFT_CANDIDATE_LOG_MIN_SIZE_DEFAULT;

  /** RW lock to manage the log roll and read access to the log */
  private final ReentrantReadWriteLock indexLock;

  /**
   *  indexed by starting index.
   */
  private TreeMap<Long, LogFileInfo> candidateLogsByIndex;

  /**
   *  indexed by base filename
   */
  private Map<String, LogFileInfo> candidateLogsByName;


  /****************************************************************************
   *                             constructor
   ****************************************************************************/
  public CandidateLogsManager(Configuration conf, String contextName, String dir) {
    this.indexLock = new ReentrantReadWriteLock();
    this.candidateLogsByIndex = new TreeMap<Long, LogFileInfo>();
    this.candidateLogsByName = new HashMap<String, LogFileInfo>();
    this.contextName = contextName;
    if (!dir.endsWith(HConstants.PATH_SEPARATOR)) {
      this.logsDirectory = dir + HConstants.PATH_SEPARATOR;
    } else {
      this.logsDirectory = dir;
    }

    updateConf(conf);

    logPrefix = this + " : ";
  }

  /**
   *  initialize
   *
   *  Just make the directories.
   */
  public void initialize() throws IOException {
    createLogDirectory(getCandidateLogsDirectory().toString());
    createLogDirectory(getCandidateLogsWorkingDirectory().toString());
  }

  public void dumpState() {
    if (!getConf().getBoolean(
          "hbase.raft.candidate.logs.manager.dump.state.enabled",
          false)
    ) {
      return;
    }
    if (isTraceEnabled()) {
      indexLock.readLock().lock();
      try {
        LOG.trace(logPrefix() + "CandidateLogsByName:");
        for (String filename : candidateLogsByName.keySet()) {
          LOG.trace(logPrefix() + filename + " => " +
            candidateLogsByName.get(filename));
        }
        LOG.trace(logPrefix() + "CandidateLogsByIndex:");
        for (Long index : candidateLogsByIndex.keySet()) {
          LOG.trace(logPrefix() + index + " => " +
            candidateLogsByIndex.get(index));
        }
      } finally {
        indexLock.readLock().unlock();
      }
    }
  }
  /****************************************************************************
   *                          invariants checker
   ****************************************************************************/
  public boolean checkInvariants() {
    long t0 = System.nanoTime();
    indexLock.readLock().lock();
    try {
      boolean passed = true;
      // every file in candidateLogsByName is in candidateLogsByIndex and vice
      // versa. We don't use Set.equals because it may be an N^2 algorithm on
      // generic types.
      dumpState();
      for (String filename : candidateLogsByName.keySet()) {
        LogFileInfo info = candidateLogsByName.get(filename);
        if (info == null) {
          LOG.error(logPrefix() + "Null value found in candidateLogsByName for filename " + filename);
          passed = false;
        } else if (!info.validate()) {
          LOG.error(logPrefix() + "Failed to validate candidate log from candidateLogsByName[" + filename + "]: " + info);
          passed = false;
        } else if (!filename.equals(info.getFilename())) {
          LOG.error(logPrefix() + "Candidate log info [" + info + "] contains a different file name than " + filename
              + ": " + info.getFilename());
          passed = false;
        } else if (candidateLogsByIndex.get(info.getInitialIndex()) == null) {
          LOG.error(logPrefix() + "Candidate log info [" + info + "] is missing from candidateLogsByIndex but present in candidateLogsByName");
          passed = false;
        } else if (!info.equals(candidateLogsByIndex.get(info.getInitialIndex()))) {
          LOG.error(logPrefix() + "Mismatched LogFileInfo found between "
              + "candidateLogsByName[" + filename + "]"
              + " and "
              + " candidateLogsByIndex[" + info.getInitialIndex() + "]"
              + " : [" + info + "] vs. [" + candidateLogsByIndex.get(info.getInitialIndex()) + "]");
          passed = false;
        }
      }

      for (Long index : candidateLogsByIndex.keySet()) {
        LogFileInfo info = candidateLogsByIndex.get(index);
        if (info == null) {
          LOG.error(logPrefix() + "Null value found in candidateLogsByIndex for index " + index);
          passed = false;
        } else if (!info.validate()) {
          LOG.error(logPrefix() + "Failed to validate candidate log from candidateLogsByIndex[" + index + "]: " + info);
          passed = false;
        } else if (index != info.getInitialIndex()) {
          LOG.error(logPrefix() + "Candidate log info [" + info + "] contains a different index than " + index);
          passed = false;
        } else if (candidateLogsByName.get(info.getFilename()) == null) {
          LOG.error(logPrefix() + "Candidate log info [" + info + "] is present in candidateLogsByIndex but missing from candidateLogsByName");
          passed = false;
        } else if (!info.equals(candidateLogsByName.get(info.getFilename()))) {
          LOG.error(logPrefix() + "Mismatched LogFileInfo found between "
              + "candidateLogsByIndex[" + index + "]"
              + " and "
              + "candidateLogsByName[" + info.getFilename() + "]"
              + " : [" + info + "] vs. [" + candidateLogsByName.get(info.getFilename()) + "]");
          passed = false;
        }
      }

      // no two files in candidateLogsByIndex overlap
      long prevLastIndex = Long.MIN_VALUE;
      for (LogFileInfo info : candidateLogsByIndex.values()) {
        if (prevLastIndex >= info.getInitialIndex()) {
          LOG.error(logPrefix() + "Previous last index " + prevLastIndex + " >= the first index of " + info);
          passed = false;
        }
        prevLastIndex = info.getLastIndex();
      }
      return passed;
    } finally {
      indexLock.readLock().unlock();
      if (CheckInvariantsLogger.isDebugEnabled()) {
        CheckInvariantsLogger.debug(logPrefix() + "checkInvariants took " + (System.nanoTime()-t0)/1000L + " us");
      }
    }
  }


  /****************************************************************************
   *                   auxiliary: conf, dirs, etc
   ****************************************************************************/
  public final synchronized void updateConf(Configuration conf) {
    this.conf = new Configuration(conf);
    minCandidateLogSize = conf.getLong(
        HConstants.RAFT_CANDIDATE_LOG_MIN_SIZE_KEY,
        HConstants.RAFT_CANDIDATE_LOG_MIN_SIZE_DEFAULT
    );
  }

  public final long getMinimumCandidateLogSize() {
    return minCandidateLogSize;
  }

  public final synchronized Configuration getConf() {
    return conf;
  }

  /**
   *  getCandidateLogsDirectory
   *
   *  Returns the absolute path of the directory holding the candidate logs.
   *
   *  @return         Path
   */
  public final Path getCandidateLogsDirectory() {
    return Paths.get(
        logsDirectory
       + getConf().get(HConstants.RAFT_CANDIDATE_LOG_DIRECTORY_NAME_KEY,
          HConstants.RAFT_CANDIDATE_LOG_DIRECTORY_NAME_DEFAULT));
  }

  /**
   *  getCandidateLogsWorkingDirectory
   *
   *  Returns the absolute path of the working directory where the fetcher(s)
   *  can store temporary candidate log files while they are being written
   *  to.
   *
   *  @return         Path
   */
  public final Path getCandidateLogsWorkingDirectory() {
    return Paths.get(
        logsDirectory
        + getConf().get(HConstants.RAFT_CANDIDATE_LOG_DIRECTORY_NAME_KEY,
          HConstants.RAFT_CANDIDATE_LOG_DIRECTORY_NAME_DEFAULT)
        + HConstants.PATH_SEPARATOR + "tmp");
  }


  /**
   *  isInCandidateLogsDirectory
   *
   *  Checks if a given file is in one of the subdirectory of the main candidate logs
   *  directory. This does not assume a flat directory.
   */
  public boolean isInCandidateLogsDirectory(File file) {
    if (file == null) {
      return false;
    }
    if (isDebugEnabled()) LOG.debug(logPrefix() + "checking if " + file.getAbsolutePath() + " is in " + getCandidateLogsDirectory());
    Path parentDir = Paths.get(file.getParentFile().getAbsolutePath());
    boolean isInCandidateLogsDirectory = false;
    while (parentDir != null) {
      if (parentDir.equals(getCandidateLogsDirectory())) {
        return true;
      }
      parentDir = parentDir.getParent();
    }
    return false;
  }

  /****************************************************************************
   *                   core routines: scan, add and I/O
   ****************************************************************************/

  /**
   *  scan
   *
   *  Scan the candidate logs directory and add files to the collection.
   */
  public void scan() throws IOException {
    List<File> candidateLogs = null;
    try {
      candidateLogs = scanCandidateLogsDirectory();
    } catch (IOException ex) {
      LOG.error(logPrefix() + "Failed to read the candidate logs directory " + getCandidateLogsDirectory(), ex);
      throw ex;
    }
    if (candidateLogs == null) {
      throw new IOException(
          "Failed to read the candidate logs directory " + getCandidateLogsDirectory());
    }
    for (File f : candidateLogs) {
      try {
        if (addLogFile(f) == null) {
          LOG.error(logPrefix() + "Failed to add file " + f.getAbsolutePath() + "; deleting it ......");
          safeDeleteFile(f);
        }
      } catch (IOException ex) {
        LOG.error(logPrefix() + "Failed to add file " + f.getAbsolutePath() + "; deleting it ......", ex);
        safeDeleteFile(f);
      }

    }
  }

  /**
   *  addLogFile
   *
   *  Add a candidate file to the collection.
   *
   *  @param  path    String
   *  @return         LogFileInfo
   */
  public LogFileInfo addLogFile(String path) throws IOException {
    if (path == null) {
      LOG.error(logPrefix() + "a null path is passed to addLogFile");
      return null;
    }
    return addLogFile(new File(path));
  }

  /**
   *  addLogFile
   *
   *  Add a file already in the candidate logs directory. No renaming will be done.
   *
   *  The possible I/O actions are truncating and deleting files.
   */
  public LogFileInfo addLogFile(File file) throws IOException {
    if (!checkFile(file)) {
      LOG.error(logPrefix() + "Invalid file passed to addLogFile: " + file);
      return null;
    }
    // we only process a file if it's in the candiate logs directory
    if (file.getParentFile().getAbsolutePath() == null) {
      LOG.error(logPrefix() + "The new candidate file " + file.getAbsolutePath()
          + " is not in any directory; weird!");
      return null;
    } else if (!isInCandidateLogsDirectory(file)) {
      LOG.error(logPrefix() + "The new candidate file " + file.getAbsolutePath() + " is in "
          + file.getParentFile().getAbsolutePath()
          + " that is different from the candidate logs directory "
          + getCandidateLogsDirectory());
      return null;
    }
    LOG.info(logPrefix() +"Adding log file " + file);
    // check if the file is already in the collection
    String filename = file.getName();
    RandomAccessLog newLogFile = null;
    indexLock.writeLock().lock();
    try {
      LogFileInfo info = candidateLogsByName.get(filename);
      if (info != null) {
        if (isDebugEnabled()) LOG.debug(logPrefix() + "File " + file.getAbsolutePath() + " is already indexed");
        return info;
      }

      LogFileInfo newFileInfo = null;
      RandomAccessLog logFile = createRandomAccessLog(file, true);
      FileLock lock = lockRandomAccessLog(logFile);
      try {
        // Rebuild the index by scanning the log file
        logFile.rebuild(toString());

        if (logFile.getInitialIndex() == HConstants.UNDEFINED_TERM_INDEX) {
          throw new IOException(
              "found invalid initial index in " + file.getAbsolutePath());
        } else if (logFile.getLastIndex() == HConstants.UNDEFINED_TERM_INDEX) {
          throw new IOException("found invalid last index in " + file.getAbsolutePath());
        }

        List<LogFileInfo> deleteTargets = new ArrayList<LogFileInfo>();
        Pair<LogFileInfo, LogFileInfo> result = computeMergeAndDelete(
            logFile.getInitialIndex(),
            logFile.getLastIndex(),
            candidateLogsByIndex,
            deleteTargets
        );
        if (result == null) {
          LOG.warn(logPrefix() + "failed to find an appropriate hole for " + logFile);
          return null;
        }
        LogFileInfo headOverlapLog = result.getFirst();
        LogFileInfo tailOverlapLog = result.getSecond();

        // Prepare the new file first by truncating if necessary.
        if (headOverlapLog == null) {
          if (tailOverlapLog != null) {
            LOG.info(logPrefix() +"truncating new log file to the initial index of tail overlap at " + tailOverlapLog.getInitialIndex());
            truncate(logFile, tailOverlapLog.getInitialIndex());
            File newFile = new File(logFile.getFile().getAbsolutePath() + ".subcopy_to_"
                + logFile.getCurrentTerm() + "_" + logFile.getInitialIndex() + "_" + (tailOverlapLog.getInitialIndex()-1));

            logFile.finalizeForWrite();

            LOG.info(logPrefix() +"Renaming the truncated file " + file.getAbsolutePath() + " to " + newFile.getAbsolutePath());
            if (!renameFile(file, newFile)) {
              LOG.error(logPrefix() + "failed to rename the truncated file " + file.getAbsolutePath() + " to " + newFile.getAbsolutePath());
              return null;
            } else {
              LOG.info(logPrefix() +"Renamed the truncated file " + file.getAbsolutePath() + " to " + newFile.getAbsolutePath());
              file = newFile;
            }
          } else {
            // We are done preparing the new file.
            logFile.finalizeForWrite();
          }
          newFileInfo = createLogFileInfo(file, logFile);
          // we still have the file locked; therefore the mod time is legit.
          newFileInfo.setLastVerifiedModificationTime(getModificationTime(file));
        } else {
          long newInitialIndex = headOverlapLog.getLastIndex() + 1;
          long newLastIndex = tailOverlapLog != null ? tailOverlapLog.getInitialIndex() - 1 : logFile.getLastIndex();

          assert newInitialIndex <= logFile.getLastIndex();
          assert newInitialIndex >= logFile.getInitialIndex();
          assert newLastIndex >= logFile.getInitialIndex();
          assert newLastIndex <= logFile.getLastIndex();

          LOG.info(logPrefix() +"About to truncate new log file " + logFile + " to the range + ["
              + newInitialIndex + ", " + newLastIndex + "]");
          if (newLastIndex + 1 - newInitialIndex < getMinimumCandidateLogSize()) {
            LOG.warn(logPrefix() + "the resultant log file would be smaller than the threshold " + getMinimumCandidateLogSize()
                + "; skip it.");
            return null;
          }
          File newFile = new File(file.getAbsolutePath() + ".subcopy_to_"
              + logFile.getCurrentTerm() + "_" + newInitialIndex + "_" + newLastIndex);
          newLogFile = subCopyRandomAccessLog(logFile, newInitialIndex, newLastIndex, newFile);
          if (newLogFile == null) {
            throw new IOException("failed to copy a subsection [" + newInitialIndex + ", " + newLastIndex
                + "] of the target random access log " + logFile);
          }
          newLogFile.finalizeForWrite();
          newFileInfo = createLogFileInfo(newFile, newLogFile);
          newFileInfo.setLastVerifiedModificationTime(getModificationTime(newFile));
          safeDeleteFile(file);
        }
        if (newFileInfo == null) {
          throw new IOException("WEIRD: we have a null newLogFileInfo!");
        }
        for (LogFileInfo deleteTarget : deleteTargets) {
          LOG.info(logPrefix() +"safe-delete the log file " + deleteTarget);
          safeDeleteCandidateLog(deleteTarget);
        }
        insertIntoCollection(newFileInfo);
        return newFileInfo;
      } catch (Exception ex) {
        LOG.error(logPrefix() + "is unable to rebuild the current log file " + logFile.getFileName(), ex);
        if (newLogFile != null) {
          LOG.warn(logPrefix() + "also deleting the new log file " + newLogFile.getFileName());
          safeDeleteFile(newLogFile.getFile());
        }
        throw new IOException("is unable to rebuild the current log file " +
          logFile.getFileName(), ex);
      } finally {
        unlockRandomAccessLog(lock);
      }
    } catch (IOException ex) {
      LOG.error(logPrefix() + "caught an exception while adding file " + file.getAbsolutePath(), ex);
    } finally {
      indexLock.writeLock().unlock();
    }
    return null;
  }

  /**
   *  safeDeleteCandidateLog
   *
   *  @param  info      LogFileInfo
   *
   *  @return boolean
   */
  public boolean safeDeleteCandidateLog(LogFileInfo info) {
    indexLock.writeLock().lock();
    try {
      removeFromCollection(info);
      return safeDeleteFile(new File(info.getAbsolutePath()));
    } finally {
      indexLock.writeLock().unlock();
    }
  }

  /**
   *  truncate
   *
   *  Truncates a RandomAccessLog file. This method exists mainly for unit tests.
   *
   *  @param  logFile   RandomAccessLog
   *  @param  index     long
   *  @return EditId    the last edit id
   */
  private EditId truncate(RandomAccessLog logFile, long index) throws IOException {
    logFile.truncate(index);
    logFile.finalizeForWrite();
    return new EditId(logFile.getCurrentTerm(), logFile.getLastIndex());
  }

  /****************************************************************************
   *                   basic I/O routines (mockable)
   ****************************************************************************/

  /**
   *  createLogDirectory
   *
   */
  protected void createLogDirectory(String path) throws IOException {
    File currentDirectory = new File (path);

    if (!currentDirectory.exists()) {
      if (!currentDirectory.mkdirs()) {
        throw new IOException("Cannot create dir at " +
          currentDirectory.getAbsolutePath());
      }
    }
  }

  /**
   *  fastCopy
   *
   *  Copy a contiguous range of transactions from one RandomAccessLog to another
   *  File by copying bytes.
   *
   *  @param  oldLog
   *  @param  newInitialIndex
   *  @param  newLastIndex
   *  @param  tmpFile
   *
   *
   */
  protected void fastCopy(
      RandomAccessLog   oldLog,
      final long        newInitialIndex,
      final long        newLastIndex,
      File              tmpFile
  ) throws IOException {
    LOG.info(logPrefix() +"fast-copying range  [" + newInitialIndex + ", " + newLastIndex
                  + "] of " + oldLog + " to tmp file " + tmpFile.getAbsolutePath());
    String sessionKey = tmpFile.getAbsolutePath();
    long term = oldLog.getCurrentTerm();
    LogReader srcReader = new LogReader(oldLog.getFile());
    try {
      RandomAccessFile srcFile = srcReader.getRandomAccessFile();
      long copyStartOffset = oldLog.getTransactionFileOffset(term, newInitialIndex);
      long copyEndOffset = newLastIndex < oldLog.getLastIndex() ?
        oldLog.getTransactionFileOffset(term, newLastIndex + 1) : srcFile.length();


      FileChannel inChannel = null;
      try {
        inChannel = new FileInputStream(oldLog.getFile()).getChannel();
        FileChannel outChannel = null;
        try {
          outChannel = new FileOutputStream(tmpFile).getChannel();

          ByteBuffer bbuf = LogWriter.generateFileHeader(term, newInitialIndex);
          do {
            outChannel.write(bbuf);
          } while (bbuf.remaining() > 0);

          LOG.info(logPrefix() +"copying the offset window ["
              + copyStartOffset + ", " + copyEndOffset + ") from "
              + oldLog + " to " + tmpFile);
          long copyOffset = copyStartOffset;
          while (copyOffset < copyEndOffset) {
            if (isDebugEnabled()) {
              LOG.debug(logPrefix() + "---- copying [" + copyOffset + ", " + copyEndOffset
                  + ") using FileChannel.transferTo ......");
            }
            long n = inChannel.transferTo(copyOffset, copyEndOffset - copyOffset, outChannel);
            if (isDebugEnabled()) {
              LOG.debug(logPrefix() + "---- copied " + n + " bytes from [" + copyOffset + ", " + copyEndOffset
                  + ") using FileChannel.transferTo.");
            }
            if (n <= 0) {
              LOG.error(logPrefix() + "FileChannel.transferTo return 0!");
              throw new IOException("FileChannel.transferTo return 0 while copying ["
                  + copyOffset + ", " + copyEndOffset + ") of " + oldLog + " to "
                  + tmpFile);
            }
            copyOffset += n;
          }
        } finally {
          if (outChannel != null) {
            outChannel.close();
          }
        }
      } finally {
        if (inChannel != null) {
          inChannel.close();
        }
      }

      // create a new log and perform some sanity test
      if (isDebugEnabled()) {
        LOG.debug(logPrefix() + "Checking the result of copying " + oldLog + " to " + tmpFile);
      }
      RandomAccessLog newLog = createRandomAccessLog(tmpFile, true);
      newLog.rebuild(sessionKey);
      if (newLog.getInitialIndex() != newInitialIndex) {
        throw new IOException("The copied file has the wrong initial index: "
            + newLog.getInitialIndex() + " vs. " + newInitialIndex);
      } else if (newLog.getLastIndex() != newLastIndex) {
        throw new IOException("The copied file has the wrong last index: "
            + newLog.getLastIndex() + " vs. " + newLastIndex);
      }
      // TODO: shall we check the file size??
    } catch (NoSuchElementException ex) {
      safeDeleteFile(tmpFile);
      throw new IOException("Invalid indices specified", ex);
    } catch (IOException ex) {
      LOG.warn(logPrefix() + "Failed to fast-copy range  [" + newInitialIndex + ", " + newLastIndex
          + "] of " + oldLog + " to tmp file " + tmpFile.getAbsolutePath(), ex);
      safeDeleteFile(tmpFile);
      throw ex;
    } finally {
      srcReader.close();
    }
  }

  /**
   *  slowCopy
   *
   *  Copy a contiguous range of transactions from one RandomAccessLog to anther
   *  File by reading and appending one transaction at a time.
   *
   *  @param  oldLog
   *  @param  newInitialIndex
   *  @param  newLastIndex
   *  @param  tmpFile
   */
  protected void slowCopy(
      RandomAccessLog   oldLog,
      final long        newInitialIndex,
      final long        newLastIndex,
      File              tmpFile
  ) throws IOException {
    LOG.info(logPrefix() +"slow-copying range  [" + newInitialIndex + ", " + newLastIndex
                  + "] of " + oldLog + " to tmp file " + tmpFile.getAbsolutePath());

    String sessionKey = tmpFile.getAbsolutePath();
    long term = oldLog.getCurrentTerm();
    LOG.debug(logPrefix() + "slowCopy term = " + term);
    try {

      RandomAccessLog newLog = new RandomAccessLog(tmpFile, false);
      for (long index=newInitialIndex; index<=newLastIndex; index++) {
        MemoryBuffer
          buffer = oldLog.getTransaction(term, index, sessionKey, null);
        if (isDebugEnabled()) {
          LOG.debug(logPrefix() + "Writing " + buffer.getBuffer().remaining()
            + " bytes at index " + index + " to " + tmpFile.getAbsolutePath());
        }
        newLog.append(term, index, buffer.getBuffer());
      }
      newLog.finalizeForWrite();
    } catch (IOException ex) {
      safeDeleteFile(tmpFile);
      throw ex;
    }
  }

  /**
   *  subCopyRandomAccessLog
   *
   *  Copy a contiguous range of transactions from one RandomAccessLog to anther
   *  File by using a temporary file as the intermediary.
   *
   *  @param  oldLog
   *  @param  newInitialIndex
   *  @param  newLastIndex
   *  @param  newFile
   */
  public RandomAccessLog subCopyRandomAccessLog(
      RandomAccessLog   oldLog,
      final long        newInitialIndex,
      final long        newLastIndex,
      File              newFile
  ) throws IOException {
    if (newFile == null || oldLog == null) {
      return null;
    }
    if (newInitialIndex > newLastIndex ||
        newInitialIndex < oldLog.getInitialIndex() ||
        newInitialIndex > oldLog.getLastIndex() ||
        newLastIndex > oldLog.getLastIndex() ||
        newLastIndex < oldLog.getInitialIndex()
    ) {
      LOG.error(logPrefix() + "Invalid range [" + newInitialIndex + ", " + newLastIndex
          + "] specified fo subcopying " + oldLog);
      return null;
    }

    File tmpFile = new File(getCandidateLogsWorkingDirectory()
        + HConstants.PATH_SEPARATOR
        + ("tmp_subcopy_at_" + System.nanoTime() + newFile.getName()));

    if (getConf().getBoolean(
          HConstants.RAFT_CANDIDATE_FAST_COPY_LOG_KEY,
          HConstants.RAFT_CANDIDATE_FAST_COPY_LOG_DEFAULT)
    ) {
      fastCopy(oldLog, newInitialIndex, newLastIndex, tmpFile);
    } else {
      slowCopy(oldLog, newInitialIndex, newLastIndex, tmpFile);
    }

    LOG.info(logPrefix() +"Renaming " + tmpFile.getAbsolutePath() + " to " + newFile.getAbsolutePath());
    if (!renameFile(tmpFile, newFile)) {
      LOG.error(logPrefix() + "Failed to rename " + tmpFile.getAbsolutePath() + " to " + newFile.getAbsolutePath());
      safeDeleteFile(tmpFile);
      safeDeleteFile(newFile);
      return null;
    }

    LOG.info(logPrefix() +"Copied range  [" + newInitialIndex + ", " + newLastIndex
                  + "] of " + oldLog + " to tmp file " + newFile.getAbsolutePath());
    return createRandomAccessLog(newFile, true);
  }

  public boolean renameFile(File oldFile, File newFile) {
    return oldFile.renameTo(newFile);
  }

  protected long getModificationTime(File f) {
    return f.lastModified();
  }

  protected LogFileInfo createLogFileInfo(File file, RandomAccessLog logFile) {
    return new LogFileInfo(
        file.getAbsolutePath(),
        file.length(),
        new EditId(logFile.getCurrentTerm(), logFile.getInitialIndex()),
        new EditId(logFile.getCurrentTerm(), logFile.getLastIndex()),
        file.lastModified(), RandomAccessLog.populateCreationTime(file)
    );
  }

  /**
   *  scanCandidateLogsDirectory
   *
   *  Scan the candidate logs directory for all log files.
   */
  protected List<File> scanCandidateLogsDirectory() throws IOException {
    List<File> files = new ArrayList<File>();
    Path candidateLogsDirectory = getCandidateLogsDirectory();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(candidateLogsDirectory)) {
      for (Path entry: stream) {
        File f = entry.toFile();
        if (checkFile(f)) {
          files.add(f);
        }
      }
    } catch (IOException ex) {
      LOG.error(logPrefix() + "Failed to read the candidate logs directory " + candidateLogsDirectory, ex);
      throw ex;
    }
    return files;
  }

  /**
   *  checkFile
   *
   *  @param  f File
   *  @return boolean
   *
   */
  protected boolean checkFile(File f) {
    return f != null && f.isFile();
  }

  /**
   *  createRandomAccessLog
   *
   *  @param  file    File
   *  @param  sync    boolean
   */
  public RandomAccessLog createRandomAccessLog(File file, boolean sync) throws IOException {
    return new RandomAccessLog(file, sync);
  }

  public RandomAccessFile createRandomAccessFile(File file, boolean isSync) throws IOException {
    return new RandomAccessFile(file, isSync ? "rws" : "rw");
  }

  public LogWriter createLogWriter(File file, boolean isSync) throws IOException {
    return new LogWriter(createRandomAccessFile(file, isSync), isSync);
  }

  /**
   *  lockRandomAccessLog
   *
   *  @param  logFile RandomAccessLog
   *  @return         FileLock
   */
  protected FileLock lockRandomAccessLog(RandomAccessLog logFile) throws IOException {
    RandomAccessFile raf = logFile.getRandomAccessFile();
    return raf.getChannel().lock();
  }

  /**
   *  unlockRandomAccessLog
   *
   *  @param  lock    FileLock
   */
  protected void unlockRandomAccessLog(FileLock lock) throws IOException {
    lock.release();
  }

  /**
   *  safeDeleteFile
   *
   *  Deletes a file represented by the file object. Only a file in the candidate logs
   *  directory can be deleted.
   *
   *  @param  file      File
   *
   *  @return boolean
   */
  public boolean safeDeleteFile(File file) {
    if (!isInCandidateLogsDirectory(file)) {
      LOG.error(logPrefix() + "The to-be-deleted file " + file.getAbsolutePath() + " is not in "
          + getCandidateLogsDirectory());
      return false;
    }
    LOG.info(logPrefix() +"deleting file " + file.getAbsolutePath());
    if (!file.delete()) {
      LOG.error(logPrefix() + "failed to delete file " + file.getAbsolutePath());
      return false;
    } else {
      return true;
    }
  }

  /****************************************************************************
   *                   core routines: collection management
   ****************************************************************************/
  public Collection<LogFileInfo> getAllScannedFiles() {
    indexLock.readLock().lock();
    try {
      return candidateLogsByIndex.values();
    } finally {
      indexLock.readLock().unlock();
    }
  }

  /**
   *  insertIntoCollection
   *
   *  Insert a log file into the collection. This doesn't cause the file to be deleted.
   */
  public void insertIntoCollection(LogFileInfo info) throws IOException {
    if (info == null) {
      return;
    }
    indexLock.writeLock().lock();
    try {
      if (candidateLogsByIndex.get(info.getInitialIndex()) != null) {
        throw new IOException("there is already a log file at index " + info.getInitialIndex());
      }
      if (candidateLogsByName.get(info.getFilename()) != null) {
        throw new IOException("there is already a log file with name " + info.getFilename());
      }
      candidateLogsByIndex.put(info.getInitialIndex(), info);
      candidateLogsByName.put(info.getFilename(), info);
    } finally {
      indexLock.writeLock().unlock();
      checkInvariants();
    }
  }

  /**
   *  removeFromCollection
   *
   *  Remove a log file from the collection. This doesn't cause the file to be deleted.
   */
  public void removeFromCollection(LogFileInfo info) {
    if (info == null) {
      return;
    }
    indexLock.writeLock().lock();
    try {
      candidateLogsByIndex.remove(info.getInitialIndex());
      candidateLogsByName.remove(info.getFilename());
    } finally {
      indexLock.writeLock().unlock();
      checkInvariants();
    }
  }

  /**
   *  getByIndex
   *
   *  Retrieves the LogFileInfo containing the index
   */
  public LogFileInfo getByIndex(long index) {
    indexLock.readLock().lock();
    try {
      // FACT: headMap contains keys <= index
      SortedMap<Long, LogFileInfo> headMap = candidateLogsByIndex.headMap(index+1);
      if (headMap != null && !headMap.isEmpty()) {
        LogFileInfo info = headMap.get(headMap.lastKey());
        if (info.getInitialIndex() <= index && info.getLastIndex() >= index) {
          return info;
        }
      }
      return null;
    } finally {
      indexLock.readLock().unlock();
    }
  }

  /**
   *  coverage
   *
   *  Computes the number of transactions current set of candidate logs
   *  contain.
   */
  public long coverage() {
    indexLock.readLock().lock();
    try {
      long coverage = 0L;
      for (Long index : candidateLogsByIndex.keySet()) {
        coverage += candidateLogsByIndex.get(index).getTxnCount();
      }
      return coverage;
    } finally {
      indexLock.readLock().unlock();
    }
  }

  /**
   *  size
   *
   *  Returns the number of indexed candidate logs.
   */
  public int size() {
    indexLock.readLock().lock();
    try {
      return candidateLogsByIndex.size();
    } finally {
      indexLock.readLock().unlock();
    }
  }

  public boolean isEmpty() {
    indexLock.readLock().lock();
    try {
      return candidateLogsByIndex.isEmpty();
    } finally {
      indexLock.readLock().unlock();
    }
  }

  public boolean exists(EditId id) {
    if (id == null) {
      return false;
    }
    LogFileInfo info = getByIndex(id.getIndex());
    return info != null && info.getFirstEditId().getTerm() <= id.getTerm()
      && id.getTerm() <= info.getLastEditId().getTerm();
  }

  /**
   *  prune
   *
   *  Delete all files with edits below index.
   */
  public void prune(long index) {
    List<LogFileInfo> targets = new ArrayList<LogFileInfo>();
    indexLock.readLock().lock();
    try {
      for (LogFileInfo info : candidateLogsByIndex.values()) {
        if (info.getLastIndex() <= index) {
          targets.add(info);
        }
      }
    } finally {
      indexLock.readLock().unlock();
    }
    for (LogFileInfo info : targets) {
      LOG.info(logPrefix() +"pruning file " + info);
      safeDeleteCandidateLog(info);
    }
  }

  /**
   *  getContiguousLogsContaining
   *
   *  @param  id                  EditId
   *  @return List<LogFileInfo>
   */
  public List<LogFileInfo> getContiguousLogsContaining(EditId id) {
    return getContiguousLogsContaining(id, Long.MAX_VALUE);
  }

  public List<LogFileInfo> getContiguousLogsContaining(EditId id, long lastLogIndex) {
    if (id == null || id.getIndex() == HConstants.UNDEFINED_TERM_INDEX) {
      return null;
    }
    List<LogFileInfo> candidates = new ArrayList<LogFileInfo>();
    indexLock.readLock().lock();
    try {
      // FACT: headMap contains keys <= id.getIndex()
      SortedMap<Long, LogFileInfo> headMap = candidateLogsByIndex.headMap(id.getIndex() + 1);
      if (headMap == null || headMap.isEmpty()) {
        return candidates;
      }
      LogFileInfo firstLog = headMap.get(headMap.lastKey());
      if (firstLog.getLastIndex() < id.getIndex()) {
        return candidates;
      }
      if (firstLog.getFirstEditId().getTerm() > id.getTerm() ||
          firstLog.getLastEditId().getTerm() < id.getTerm())
      {
        return candidates;
      }

      candidates.add(firstLog);

      // FACT: tailMap contains keys > firstLog.getLastIndex()
      SortedMap<Long, LogFileInfo> tailMap = candidateLogsByIndex.tailMap(firstLog.getLastIndex() + 1);
      if (tailMap == null || tailMap.isEmpty()) {
        return candidates;
      }

      for (LogFileInfo nextLog : tailMap.values()) {
        if (nextLog.getLastIndex() > lastLogIndex) {
          break;
        }
        if (nextLog.getInitialIndex() == candidates.get(candidates.size()-1).getLastIndex() + 1) {
          candidates.add(nextLog);
        } else {
          break;
        }
      }
      return candidates;
    } finally {
      indexLock.readLock().unlock();
    }
  }

  public String toString() {
    return "CandidatesLogManager[" + contextName + "]";
  }

  /**
   *  Invariants
   *
   *  Merge Invariants.
   *
   *  1.  initialIndex <= lastIndex
   *  2.  headOverlapLog == null || initialIndex <= headOverlapLog.getLastIndex() < lastIndex
   *  3.  tailOverlapLog == null || tailOverlapLog.getInitialIndex() <= lastIndex
   *  4.  (tailOverlapLog != null && headOverlapLog != null) ==>
   *      headOverlapLog.getLastIndex() + 1 < tailOverlapLog.getInitialIndex()
   *  5.  foreach log in deleteTargets
   *        log.getInitialIndex() > initialIndex && log.getLastIndex() <= lastIndex
   */
  public void checkMergeInvariants(
      long                          initialIndex,
      long                          lastIndex,
      TreeMap<Long, LogFileInfo>    logs,
      LogFileInfo                   headOverlapLog,
      LogFileInfo                   tailOverlapLog,
      List<LogFileInfo>             deleteTargets
  ) {
    if (isDebugEnabled()) LOG.debug(logPrefix() + "checkMergeInvariants: "
        + "initialIndex=" + initialIndex
        + ", lastIndex=" + lastIndex
        + ", headOverlapLog=" + headOverlapLog
        + ", tailOverlapLog=" + tailOverlapLog
        + "\ndeleteTargets=" + deleteTargets
    );
    dumpState();

    assert initialIndex <= lastIndex;

    // note the head overlap may be extended in the walk
    assert headOverlapLog == null ||
      (initialIndex <= headOverlapLog.getLastIndex() && lastIndex > headOverlapLog.getLastIndex());

    assert tailOverlapLog == null || tailOverlapLog.getInitialIndex() <= lastIndex;

    if (tailOverlapLog != null && headOverlapLog != null) {
      assert headOverlapLog.getLastIndex() + 1 < tailOverlapLog.getInitialIndex();
    }
    for (LogFileInfo log : deleteTargets) {
      assert log.getInitialIndex() > initialIndex && log.getLastIndex() <= lastIndex;
    }
  }

  /**
   *  computeMergeAndDelete
   *
   *  Given a set of existing candidate logs (non-overlapping) and the initial and the last
   *  indices of a new candidate log, computes one of the following.
   *
   *  1. If the new log partially or completely fills a hole in the existing set's coverage,
   *     returns the existing logs that partially overlap the beginning and the end of the
   *     new log, respectively, as well as any existing candidate logs that are fully subsumed
   *     by this new log.
   *  2. Otherwise, the new log file's coverage is already contained in the existing set;
   *     returns null.
   *
   *  Isolated for unit testing.
   */
  public Pair<LogFileInfo, LogFileInfo> computeMergeAndDelete(
      long                          initialIndex,
      long                          lastIndex,
      TreeMap<Long, LogFileInfo>    logs,
      List<LogFileInfo>             deleteTargets
  ) throws IOException {
    LogFileInfo headOverlapLog = null;

    // FACT: headMap contains keys <= initialIndex
    SortedMap<Long, LogFileInfo> headMap = logs.headMap(initialIndex+1);

    if (headMap != null && !headMap.isEmpty()) {
      LogFileInfo log = headMap.get(headMap.lastKey());
      if (lastIndex <= log.getLastIndex()) {
        LOG.info(logPrefix() +" found an existing candidate log file " + log
             + " that already covers the range of a new candidate file [" + initialIndex + ", " + lastIndex + "]");
        return null;
      } else if (log.getLastIndex() >= initialIndex) {
        headOverlapLog = log;
        if (isDebugEnabled()) LOG.debug(logPrefix() + " found an existing candidate log file " + headOverlapLog
            + " that overlaps the range of a new candidate file [" + initialIndex + ", " + lastIndex + "]");
      }
    }
    checkMergeInvariants(initialIndex, lastIndex, logs, headOverlapLog, null, deleteTargets);

    // FACT: tailMap contains keys > initialIndex
    SortedMap<Long, LogFileInfo> tailMap = logs.tailMap(initialIndex+1);

    // FACT: headMap and tailMap are disjoint

    if (tailMap != null && !tailMap.isEmpty()) {
      tailMap = tailMap.headMap(lastIndex + 1);
    }

    // FACT: tailMap contains keys > initialIndex and <= lastIndex

    // FACT: headMap and tailMap are disjoint

    if (tailMap == null || tailMap.isEmpty()) {
      checkMergeInvariants(initialIndex, lastIndex, logs, headOverlapLog, null, deleteTargets);
      return new Pair<LogFileInfo, LogFileInfo>(headOverlapLog, null);
    }

    LogFileInfo tailOverlapLog = null;
    for (LogFileInfo thisLogFile : tailMap.values()) {
      // FACT: thisLogFile.getInitialIndex() > initialIndex
      if (headOverlapLog != null && (headOverlapLog.getLastIndex() + 1) == thisLogFile.getInitialIndex()) {
        // existing log files abut, update headOverlapLogLastIndex
        LOG.info(logPrefix() +"Existing head overlap " + headOverlapLog
            + " and next log file " + thisLogFile + " abut!");
        if (thisLogFile.getLastIndex() >= lastIndex) {
          LOG.info(logPrefix() +" found an existing set of candidate log files until " + headOverlapLog
               + " that already cover the range of a new candidate file [" + initialIndex + ", " + lastIndex + "]");
          return null;
        }
        headOverlapLog = thisLogFile;
      } else if (headOverlapLog != null && headOverlapLog.getLastIndex() >= thisLogFile.getInitialIndex()) {
        // ERROR!!
        LOG.error(logPrefix() + "detected overlapping ranges at " + thisLogFile);
        return null;
      } else if (thisLogFile.getLastIndex() <= lastIndex) {
        //  FACT: (headOverlapLogLastIndex == null || headOverlapLogLastIndex  + 1 < thisLogFile.getInitialIndex())
        //        && thisLogFile.getLastIndex() <= lastIndex
        //
        //  Action: We can delete this file
        if (isDebugEnabled()) LOG.debug(logPrefix() + " found an existing candidate log " + thisLogFile
            + " that is fully contained by our new candidate file [" + initialIndex + ", " + lastIndex + "]");
        deleteTargets.add(thisLogFile);
      } else {
        //        headOverlapLogLastIndex == null
        //    &&  thisLogFile.getInitialIndex() > initialIndex
        //    &&  thisLogFile.getLastIndex() > lastIndex
        // OR
        //        initialIndex  <= headOverlapLogLastIndex
        //    &&  headOverlapLogLastIndex + 1 < thisLogFile.getInitialIndex()
        //    &&  lastIndex < thisLogFile.getLastIndex()
        if (isDebugEnabled()) LOG.debug(logPrefix() + " found a log file " + thisLogFile
            + " that overlaps the tail of our new candidate file [" + initialIndex + ", " + lastIndex + "]");
        tailOverlapLog = thisLogFile;
        break;
      }
      checkMergeInvariants(initialIndex, lastIndex, logs, headOverlapLog, tailOverlapLog, deleteTargets);
    }
    checkMergeInvariants(initialIndex, lastIndex, logs, headOverlapLog, tailOverlapLog, deleteTargets);
    return new Pair<LogFileInfo, LogFileInfo>(headOverlapLog, tailOverlapLog);
  }

  private boolean isTraceEnabled() {
    return LOG.isTraceEnabled();
  }

  private boolean isDebugEnabled() {
    return LOG.isDebugEnabled();
  }

  private String logPrefix() {
    return logPrefix;
  }
}
