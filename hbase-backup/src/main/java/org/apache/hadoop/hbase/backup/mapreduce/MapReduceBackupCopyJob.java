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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyJob;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map-Reduce implementation of {@link BackupCopyJob}. Basically, there are 2 types of copy
 * operation: one is copying from snapshot, which bases on extending ExportSnapshot's function, the
 * other is copying for incremental log files, which bases on extending DistCp's function.
 */
@InterfaceAudience.Private
public class MapReduceBackupCopyJob implements BackupCopyJob {
  public static final String NUMBER_OF_LEVELS_TO_PRESERVE_KEY = "num.levels.preserve";

  // This prefix specifies the DistCp options to be used during backup copy
  public static final String BACKUP_COPY_OPTION_PREFIX = "hbase.backup.copy.";

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceBackupCopyJob.class);

  private Configuration conf;

  // Accumulated progress within the whole backup process for the copy operation
  private float progressDone = 0.1f;
  private long bytesCopied = 0;
  private static float INIT_PROGRESS = 0.1f;

  // The percentage of the current copy task within the whole task if multiple time copies are
  // needed. The default value is 100%, which means only 1 copy task for the whole.
  private float subTaskPercntgInWholeTask = 1f;

  public MapReduceBackupCopyJob() {
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the current copy task percentage within the whole task if multiple copies are needed.
   * @return the current copy task percentage
   */
  public float getSubTaskPercntgInWholeTask() {
    return subTaskPercntgInWholeTask;
  }

  /**
   * Set the current copy task percentage within the whole task if multiple copies are needed. Must
   * be called before calling
   * {@link #copy(BackupInfo, BackupManager, Configuration, BackupType, String[])}
   * @param subTaskPercntgInWholeTask The percentage of the copy subtask
   */
  public void setSubTaskPercntgInWholeTask(float subTaskPercntgInWholeTask) {
    this.subTaskPercntgInWholeTask = subTaskPercntgInWholeTask;
  }

  static class SnapshotCopy extends ExportSnapshot {
    private BackupInfo backupInfo;
    private TableName table;

    public SnapshotCopy(BackupInfo backupInfo, TableName table) {
      super();
      this.backupInfo = backupInfo;
      this.table = table;
    }

    public TableName getTable() {
      return this.table;
    }

    public BackupInfo getBackupInfo() {
      return this.backupInfo;
    }
  }

  /**
   * Update the ongoing backup with new progress.
   * @param backupInfo  backup info
   * @param newProgress progress
   * @param bytesCopied bytes copied
   * @throws IOException exception
   */
  static void updateProgress(BackupInfo backupInfo, BackupManager backupManager, int newProgress,
    long bytesCopied) throws IOException {
    // compose the new backup progress data, using fake number for now
    String backupProgressData = newProgress + "%";

    backupInfo.setProgress(newProgress);
    backupManager.updateBackupInfo(backupInfo);
    LOG.debug("Backup progress data \"" + backupProgressData
      + "\" has been updated to backup system table for " + backupInfo.getBackupId());
  }

  /**
   * Extends DistCp for progress updating to backup system table during backup. Using DistCpV2
   * (MAPREDUCE-2765). Simply extend it and override execute() method to get the Job reference for
   * progress updating. Only the argument "src1, [src2, [...]] dst" is supported, no more DistCp
   * options.
   */

  class BackupDistCp extends DistCp {

    private BackupInfo backupInfo;
    private BackupManager backupManager;

    public BackupDistCp(Configuration conf, DistCpOptions options, BackupInfo backupInfo,
      BackupManager backupManager) throws Exception {
      super(conf, options);
      this.backupInfo = backupInfo;
      this.backupManager = backupManager;
    }

    @Override
    public Job execute() throws Exception {

      // reflection preparation for private methods and fields
      Class<?> classDistCp = org.apache.hadoop.tools.DistCp.class;
      Method methodCleanup = classDistCp.getDeclaredMethod("cleanup");

      Field fieldInputOptions = getInputOptionsField(classDistCp);
      Field fieldSubmitted = classDistCp.getDeclaredField("submitted");

      methodCleanup.setAccessible(true);
      fieldInputOptions.setAccessible(true);
      fieldSubmitted.setAccessible(true);

      // execute() logic starts here
      assert fieldInputOptions.get(this) != null;

      Job job = null;
      try {

        List<Path> srcs = InputFileListingGenerator.getSourcePaths(this, fieldInputOptions);

        long totalSrcLgth = 0;
        for (Path aSrc : srcs) {
          totalSrcLgth += BackupUtils.getFilesLength(aSrc.getFileSystem(super.getConf()), aSrc);
        }

        // Async call
        job = super.execute();
        // Update the copy progress to system table every 0.5s if progress value changed
        int progressReportFreq = MapReduceBackupCopyJob.this.getConf()
          .getInt("hbase.backup.progressreport.frequency", 500);
        float lastProgress = progressDone;
        while (!job.isComplete()) {
          float newProgress =
            progressDone + job.mapProgress() * subTaskPercntgInWholeTask * (1 - INIT_PROGRESS);

          if (newProgress > lastProgress) {

            BigDecimal progressData =
              new BigDecimal(newProgress * 100).setScale(1, BigDecimal.ROUND_HALF_UP);
            String newProgressStr = progressData + "%";
            LOG.info("Progress: " + newProgressStr);
            updateProgress(backupInfo, backupManager, progressData.intValue(), bytesCopied);
            LOG.debug("Backup progress data updated to backup system table: \"Progress: "
              + newProgressStr + ".\"");
            lastProgress = newProgress;
          }
          Thread.sleep(progressReportFreq);
        }
        // update the progress data after copy job complete
        float newProgress =
          progressDone + job.mapProgress() * subTaskPercntgInWholeTask * (1 - INIT_PROGRESS);
        BigDecimal progressData =
          new BigDecimal(newProgress * 100).setScale(1, BigDecimal.ROUND_HALF_UP);

        String newProgressStr = progressData + "%";
        LOG.info("Progress: " + newProgressStr + " subTask: " + subTaskPercntgInWholeTask
          + " mapProgress: " + job.mapProgress());

        // accumulate the overall backup progress
        progressDone = newProgress;
        bytesCopied += totalSrcLgth;

        updateProgress(backupInfo, backupManager, progressData.intValue(), bytesCopied);
        LOG.debug("Backup progress data updated to backup system table: \"Progress: "
          + newProgressStr + " - " + bytesCopied + " bytes copied.\"");
      } catch (Throwable t) {
        LOG.error(t.toString(), t);
        throw t;
      }

      String jobID = job.getJobID().toString();
      job.getConfiguration().set(DistCpConstants.CONF_LABEL_DISTCP_JOB_ID, jobID);

      LOG.debug(
        "DistCp job-id: " + jobID + " completed: " + job.isComplete() + " " + job.isSuccessful());
      Counters ctrs = job.getCounters();
      LOG.debug(Objects.toString(ctrs));
      if (job.isComplete() && !job.isSuccessful()) {
        throw new Exception("DistCp job-id: " + jobID + " failed");
      }

      return job;
    }

    private Field getInputOptionsField(Class<?> classDistCp) throws IOException {
      Field f = null;
      try {
        f = classDistCp.getDeclaredField("inputOptions");
      } catch (Exception e) {
        // Haddop 3
        try {
          f = classDistCp.getDeclaredField("context");
        } catch (NoSuchFieldException | SecurityException e1) {
          throw new IOException(e1);
        }
      }
      return f;
    }

    @Override
    protected Path createInputFileListing(Job job) throws IOException {

      if (conf.get(NUMBER_OF_LEVELS_TO_PRESERVE_KEY) == null) {
        return super.createInputFileListing(job);
      }

      int levels = conf.getInt(NUMBER_OF_LEVELS_TO_PRESERVE_KEY, 1);
      return InputFileListingGenerator.createInputFileListing(this, job, conf, getFileListingPath(),
        levels);
    }
  }

  /**
   * Do backup copy based on different types.
   * @param context  The backup info
   * @param conf     The hadoop configuration
   * @param copyType The backup copy type
   * @param options  Options for customized ExportSnapshot or DistCp
   * @throws IOException exception
   */
  @Override
  public int copy(BackupInfo context, BackupManager backupManager, Configuration conf,
    BackupType copyType, String[] options) throws IOException {
    int res = 0;

    try {
      if (copyType == BackupType.FULL) {
        SnapshotCopy snapshotCp = new SnapshotCopy(context, context.getTableBySnapshot(options[1]));
        LOG.debug("Doing SNAPSHOT_COPY");
        // Make a new instance of conf to be used by the snapshot copy class.
        snapshotCp.setConf(new Configuration(conf));
        res = snapshotCp.run(options);

      } else if (copyType == BackupType.INCREMENTAL) {
        LOG.debug("Doing COPY_TYPE_DISTCP");
        setSubTaskPercntgInWholeTask(1f);

        BackupDistCp distcp =
          new BackupDistCp(new Configuration(conf), null, context, backupManager);
        // Handle a special case where the source file is a single file.
        // In this case, distcp will not create the target dir. It just take the
        // target as a file name and copy source file to the target (as a file name).
        // We need to create the target dir before run distcp.
        LOG.debug("DistCp options: " + Arrays.toString(options));
        Path dest = new Path(options[options.length - 1]);
        String[] newOptions = new String[options.length + 1];
        System.arraycopy(options, 0, newOptions, 1, options.length);
        newOptions[0] = "-async"; // run DisCp in async mode
        FileSystem destfs = dest.getFileSystem(conf);
        if (!destfs.exists(dest)) {
          destfs.mkdirs(dest);
        }

        List<String> distCpOptionsFromConf = parseDistCpOptions(conf);
        String[] finalOptions = new String[newOptions.length + distCpOptionsFromConf.size()];
        for (int i = 0; i < distCpOptionsFromConf.size(); i++) {
          finalOptions[i] = distCpOptionsFromConf.get(i);
        }
        System.arraycopy(newOptions, 0, finalOptions, distCpOptionsFromConf.size(),
          newOptions.length);
        res = distcp.run(finalOptions);
      }
      return res;

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void cancel(String jobId) throws IOException {
    JobID id = JobID.forName(jobId);
    Cluster cluster = new Cluster(this.getConf());
    try {
      Job job = cluster.getJob(id);
      if (job == null) {
        LOG.error("No job found for " + id);
        // should we throw exception
        return;
      }
      if (job.isComplete() || job.isRetired()) {
        return;
      }

      job.killJob();
      LOG.debug("Killed copy job " + id);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  protected static List<String> parseDistCpOptions(Configuration conf) {
    List<String> extraArgsFromConf = new ArrayList<>();

    for (DistCpOptionSwitch optionSwitch : DistCpOptionSwitch.values()) {
      String configLabel = BACKUP_COPY_OPTION_PREFIX + optionSwitch.getConfigLabel();
      if (conf.get(configLabel) != null) {
        if (optionSwitch.getOption().hasArg()) {
          extraArgsFromConf.add("-" + optionSwitch.getOption().getOpt());
          extraArgsFromConf.add(conf.get(configLabel));
        } else {
          boolean value = conf.getBoolean(configLabel, false);
          if (value) {
            extraArgsFromConf.add("-" + optionSwitch.getOption().getOpt());
          }
        }
      }
    }

    return extraArgsFromConf;
  }

}
