package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

import java.util.Objects;

@InterfaceAudience.Public
public final class FileBasedStoreFileCleanerStatus {
  private long lastRuntime;
  private long minRuntime = Long.MAX_VALUE;
  private long maxRuntime = Long.MIN_VALUE;
  private long deletedFiles;
  private long failedDeletes;
  private long runs;

  public FileBasedStoreFileCleanerStatus(){
  }

  public FileBasedStoreFileCleanerStatus(long lastRuntime, long minRuntime, long maxRuntime,
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
    FileBasedStoreFileCleanerStatus that = (FileBasedStoreFileCleanerStatus) o;
    return lastRuntime == that.lastRuntime && minRuntime == that.minRuntime
      && maxRuntime == that.maxRuntime && deletedFiles == that.deletedFiles
      && failedDeletes == that.failedDeletes && runs == that.runs;
  }

  @Override public int hashCode() {
    return Objects.hash(lastRuntime, minRuntime, maxRuntime, deletedFiles, failedDeletes, runs);
  }
}
