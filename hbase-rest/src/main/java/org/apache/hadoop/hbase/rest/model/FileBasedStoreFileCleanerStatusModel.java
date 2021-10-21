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
package org.apache.hadoop.hbase.rest.model;

import org.apache.yetus.audience.InterfaceAudience;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Objects;

@XmlRootElement(name = "FileBasedStoreFileCleanerStatus") @XmlAccessorType(XmlAccessType.FIELD)
@InterfaceAudience.Private
public final class FileBasedStoreFileCleanerStatusModel implements Serializable {
  private long lastRuntime;
  private long minRuntime = Long.MAX_VALUE;
  private long maxRuntime = Long.MIN_VALUE;
  private long deletedFiles;
  private long failedDeletes;
  private long runs;

  public FileBasedStoreFileCleanerStatusModel(){
  }

  public FileBasedStoreFileCleanerStatusModel(long lastRuntime, long minRuntime, long maxRuntime,
    long deletedFiles, long failedDeletes, long runs) {
    this.lastRuntime = lastRuntime;
    this.minRuntime = minRuntime;
    this.maxRuntime = maxRuntime;
    this.deletedFiles = deletedFiles;
    this.failedDeletes = failedDeletes;
    this.runs = runs;
  }

  public void updateRuntime(long runtime) {
    this.lastRuntime = runtime;
    if(minRuntime > runtime){
      minRuntime = runtime;
    }
    if (maxRuntime < runtime){
      maxRuntime = runtime;
    }
  }

  public void incrementDeletedFiles(long deletedFiles) {
    this.deletedFiles += deletedFiles;
  }

  public void incrementFailedDeletes(long failedDeletes) {
    this.failedDeletes += failedDeletes;
  }

  public void incrementRuns(long runs) {
    this.runs += runs;
  }

  public long getLastRuntime() {
    return lastRuntime;
  }

  public long getMinRuntime() {
    return minRuntime;
  }

  public long getMaxRuntime() {
    return maxRuntime;
  }

  public long getDeletedFiles() {
    return deletedFiles;
  }

  public long getFailedDeletes() {
    return failedDeletes;
  }

  public long getRuns() {
    return runs;
  }

  @Override public String toString() {
    return "FileBasedStoreFileCleanerStatus{" + "lastRuntime=" + lastRuntime + ", minRuntime="
      + minRuntime + ", maxRuntime=" + maxRuntime + ", deletedFiles=" + deletedFiles
      + ", failedDeletes=" + failedDeletes + ", runs=" + runs + '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileBasedStoreFileCleanerStatusModel that = (FileBasedStoreFileCleanerStatusModel) o;
    return lastRuntime == that.lastRuntime && minRuntime == that.minRuntime
      && maxRuntime == that.maxRuntime && deletedFiles == that.deletedFiles
      && failedDeletes == that.failedDeletes && runs == that.runs;
  }

  @Override public int hashCode() {
    return Objects.hash(lastRuntime, minRuntime, maxRuntime, deletedFiles, failedDeletes, runs);
  }
}
