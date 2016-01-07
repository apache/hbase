/*
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

package org.apache.hadoop.hbase;

import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;


/**
 * Check the resources used:
 * - threads
 * - file descriptor
 */
public class ResourceChecker {
  private static final Log LOG = LogFactory.getLog(ResourceChecker.class);

  enum Phase {
    INITIAL, INTERMEDIATE, END
  }
  private static Set<String> initialThreadNames = new HashSet<String>();

  /**
   * On unix, we know how to get the number of open file descriptor
   */
  private static class ResourceAnalyzer {
    private static final OperatingSystemMXBean osStats;
    private static final UnixOperatingSystemMXBean unixOsStats;

    public long getThreadsCount(Phase phase) {
      Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
      if (phase == Phase.INITIAL) {
        for (Thread t : stackTraces.keySet()) {
          initialThreadNames.add(t.getName());
        }
      }
      return stackTraces.size();
    }

    public long getOpenFileDescriptorCount() {
      if (unixOsStats == null) {
        return 0;
      } else {
        return unixOsStats.getOpenFileDescriptorCount();
      }
    }

    public long getMaxFileDescriptorCount() {
      if (unixOsStats == null) {
        return 0;
      } else {
        return unixOsStats.getMaxFileDescriptorCount();
      }
    }

    public long getConnectionCount(){
      return HConnectionTestingUtility.getConnectionCount();
    }

    static {
      osStats =
        ManagementFactory.getOperatingSystemMXBean();
      if (osStats instanceof UnixOperatingSystemMXBean) {
        unixOsStats = (UnixOperatingSystemMXBean) osStats;
      } else {
        unixOsStats = null;
      }
    }
  }

  private static final ResourceAnalyzer rc = new ResourceAnalyzer();

  /**
   * Maximum we set for the thread. Will get a warning in logs
   * if we go other this limit
   */
  private static final long MAX_THREADS_COUNT = 500;

  /**
   * Maximum we set for the thread. Will get a warning in logs
   * if we go other this limit
   */
  private static final long MAX_FILE_HANDLES_COUNT = 1024;


  private long initialThreadsCount;
  private long initialFileHandlesCount;
  private long initialConnectionCount;


  public boolean checkThreads(String tagLine) {
    boolean isOk = true;
    long threadCount = rc.getThreadsCount(Phase.INTERMEDIATE);

    if (threadCount > MAX_THREADS_COUNT) {
      LOG.error(
        tagLine + ": too many threads used. We use " +
          threadCount + " our max is " + MAX_THREADS_COUNT);
      isOk = false;
    }
    return isOk;
  }

  public boolean check(String tagLine) {

    boolean isOk = checkThreads(tagLine);
    if (!checkFileHandles(tagLine)) isOk = false;

    return isOk;
  }

  public ResourceChecker(String tagLine) {
    init(tagLine);
  }

  public final void init(String tagLine) {
    if (rc.getMaxFileDescriptorCount() < MAX_FILE_HANDLES_COUNT) {
      LOG.error(
        "Bad configuration: the operating systems file handles maximum is " +
          rc.getMaxFileDescriptorCount() + " our is " + MAX_FILE_HANDLES_COUNT);
    }

    logInfo(Phase.INITIAL, tagLine);

    initialThreadsCount = rc.getThreadsCount(Phase.INITIAL);
    initialFileHandlesCount = rc.getOpenFileDescriptorCount();
    initialConnectionCount= rc.getConnectionCount();

    check(tagLine);
  }

  public void logInfo(Phase phase, String tagLine) {
    long threadCount = rc.getThreadsCount(phase);
    LOG.info(
        tagLine + ": " +
        threadCount + " threads" +
        (initialThreadsCount > 0 ?
          " (was " + initialThreadsCount + "), " : ", ") +
        rc.getOpenFileDescriptorCount() + " file descriptors" +
        (initialFileHandlesCount > 0 ?
          " (was " + initialFileHandlesCount + "). " : " ") +
        rc.getConnectionCount() + " connections" +
        (initialConnectionCount > 0 ?
          " (was " + initialConnectionCount + "), " : ", ") +
        (initialThreadsCount > 0 && threadCount > initialThreadsCount ?
          " -thread leak?- " : "") +
        (initialFileHandlesCount > 0 &&
          rc.getOpenFileDescriptorCount() > initialFileHandlesCount ?
          " -file handle leak?- " : "") +
        (initialConnectionCount > 0 &&
          rc.getConnectionCount() > initialConnectionCount ?
          " -connection leak?- " : "" )
    );
    if (phase == Phase.END) {
      Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
      if (stackTraces.size() > initialThreadNames.size()) {
        for (Thread t : stackTraces.keySet()) {
          if (!initialThreadNames.contains(t.getName())) {
            LOG.info(tagLine + ": potentially hanging thread - " + t.getName());
            StackTraceElement[] stackElements = stackTraces.get(t);
            for (StackTraceElement ele : stackElements) {
              LOG.info("\t" + ele);
            }
          }
        }
      }
    }
  }


  public boolean checkFileHandles(String tagLine) {
    boolean isOk = true;

    if (rc.getOpenFileDescriptorCount() > MAX_FILE_HANDLES_COUNT) {
      LOG.error(
        tagLine + ": too many file handles used. We use " +
          rc.getOpenFileDescriptorCount() + " our max is " +
          MAX_FILE_HANDLES_COUNT);
      isOk = false;
    }

    return isOk;
  }

  /**
   * Helper function: print the threads
   */
  public static void printThreads(){
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    System.out.println("name; state; isDameon; isAlive; isInterrupted");
    for (Thread t: threads){
      System.out.println(
        t.getName()+";"+t.getState()+";"+t.isDaemon()+";"+t.isAlive()+
          ";"+t.isInterrupted()
      );
    }
  }
}
