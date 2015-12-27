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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.BackupCopyService;
import org.apache.hadoop.hbase.backup.BackupHandler;
import org.apache.hadoop.hbase.backup.BackupUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
/**
 * Copier for backup operation. Basically, there are 2 types of copy. One is copying from snapshot,
 * which bases on extending ExportSnapshot's function with copy progress reporting to ZooKeeper
 * implementation. The other is copying for incremental log files, which bases on extending 
 * DistCp's function with copy progress reporting to ZooKeeper implementation.
 *
 * For now this is only a wrapper. The other features such as progress and increment backup will be
 * implemented in future jira
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MapReduceBackupCopyService implements BackupCopyService {
  private static final Log LOG = LogFactory.getLog(MapReduceBackupCopyService.class);

  private Configuration conf;
  // private static final long BYTES_PER_MAP = 2 * 256 * 1024 * 1024;

  // Accumulated progress within the whole backup process for the copy operation
  private float progressDone = 0.1f;
  private long bytesCopied = 0;
  private static float INIT_PROGRESS = 0.1f;

  // The percentage of the current copy task within the whole task if multiple time copies are
  // needed. The default value is 100%, which means only 1 copy task for the whole.
  private float subTaskPercntgInWholeTask = 1f;

  public MapReduceBackupCopyService() {
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
   * {@link #copy(BackupHandler, Configuration, Type, String[])}
   * @param subTaskPercntgInWholeTask The percentage of the copy subtask
   */
  public void setSubTaskPercntgInWholeTask(float subTaskPercntgInWholeTask) {
    this.subTaskPercntgInWholeTask = subTaskPercntgInWholeTask;
  }

  class SnapshotCopy extends ExportSnapshot {
    private BackupHandler backupHandler;
    private String table;

    public SnapshotCopy(BackupHandler backupHandler, String table) {
      super();
      this.backupHandler = backupHandler;
      this.table = table;
    }

    public BackupHandler getBackupHandler() {
      return this.backupHandler;
    }

    public String getTable() {
      return this.table;
    }
  }  
  
  // Extends DistCp for progress updating to hbase:backup
  // during backup. Using DistCpV2 (MAPREDUCE-2765).
  // Simply extend it and override execute() method to get the 
  // Job reference for progress updating.
  // Only the argument "src1, [src2, [...]] dst" is supported, 
  // no more DistCp options.
  class BackupDistCp extends DistCp {

    private BackupHandler backupHandler;

    public BackupDistCp(Configuration conf, DistCpOptions options, BackupHandler backupHandler)
        throws Exception {
      super(conf, options);
      this.backupHandler = backupHandler;
    }

    @Override
    public Job execute() throws Exception {

      // reflection preparation for private methods and fields
      Class<?> classDistCp = org.apache.hadoop.tools.DistCp.class;
      Method methodCreateMetaFolderPath = classDistCp.getDeclaredMethod("createMetaFolderPath");
      Method methodCreateJob = classDistCp.getDeclaredMethod("createJob");
      Method methodCreateInputFileListing =
          classDistCp.getDeclaredMethod("createInputFileListing", Job.class);
      Method methodCleanup = classDistCp.getDeclaredMethod("cleanup");

      Field fieldInputOptions = classDistCp.getDeclaredField("inputOptions");
      Field fieldMetaFolder = classDistCp.getDeclaredField("metaFolder");
      Field fieldJobFS = classDistCp.getDeclaredField("jobFS");
      Field fieldSubmitted = classDistCp.getDeclaredField("submitted");

      methodCreateMetaFolderPath.setAccessible(true);
      methodCreateJob.setAccessible(true);
      methodCreateInputFileListing.setAccessible(true);
      methodCleanup.setAccessible(true);

      fieldInputOptions.setAccessible(true);
      fieldMetaFolder.setAccessible(true);
      fieldJobFS.setAccessible(true);
      fieldSubmitted.setAccessible(true);

      // execute() logic starts here
      assert fieldInputOptions.get(this) != null;
      assert getConf() != null;

      Job job = null;
      try {
        synchronized (this) {
          // Don't cleanup while we are setting up.
          fieldMetaFolder.set(this, methodCreateMetaFolderPath.invoke(this));
          fieldJobFS.set(this, ((Path) fieldMetaFolder.get(this)).getFileSystem(getConf()));

          job = (Job) methodCreateJob.invoke(this);
        }
        methodCreateInputFileListing.invoke(this, job);

        // Get the total length of the source files
        List<Path> srcs = ((DistCpOptions) fieldInputOptions.get(this)).getSourcePaths();
        long totalSrcLgth = 0;
        for (Path aSrc : srcs) {
          totalSrcLgth += BackupUtil.getFilesLength(aSrc.getFileSystem(getConf()), aSrc);
        }

        // submit the copy job
        job.submit();
        fieldSubmitted.set(this, true);

        // after submit the MR job, set its handler in backup handler for cancel process
        // this.backupHandler.copyJob = job;

        // Update the copy progress to ZK every 0.5s if progress value changed
        int progressReportFreq =
            this.getConf().getInt("hbase.backup.progressreport.frequency", 500);
        float lastProgress = progressDone;
        while (!job.isComplete()) {
          float newProgress =
              progressDone + job.mapProgress() * subTaskPercntgInWholeTask * (1 - INIT_PROGRESS);

          if (newProgress > lastProgress) {

            BigDecimal progressData =
                new BigDecimal(newProgress * 100).setScale(1, BigDecimal.ROUND_HALF_UP);
            String newProgressStr = progressData + "%";
            LOG.info("Progress: " + newProgressStr);
            this.backupHandler.updateProgress(newProgressStr, bytesCopied);
            LOG.debug("Backup progress data updated to hbase:backup: \"Progress: " + newProgressStr
              + ".\"");
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
        LOG.info("Progress: " + newProgressStr);

        // accumulate the overall backup progress
        progressDone = newProgress;
        bytesCopied += totalSrcLgth;

        this.backupHandler.updateProgress(newProgressStr, bytesCopied);
        LOG.debug("Backup progress data updated to hbase:backup: \"Progress: " + newProgressStr
          + " - " + bytesCopied + " bytes copied.\"");

      } finally {
        if (!fieldSubmitted.getBoolean(this)) {
          methodCleanup.invoke(this);
        }
      }

      String jobID = job.getJobID().toString();
      job.getConfiguration().set(DistCpConstants.CONF_LABEL_DISTCP_JOB_ID, jobID);

      LOG.debug("DistCp job-id: " + jobID);
      return job;
    }

  }

  /**
   * Do backup copy based on different types.
   * @param handler The backup handler reference
   * @param conf The hadoop configuration
   * @param copyType The backup copy type
   * @param options Options for customized ExportSnapshot or DistCp
   * @throws Exception exception
   */
  public int copy(BackupHandler handler, Configuration conf, BackupCopyService.Type copyType,
      String[] options) throws IOException {

    int res = 0;

    try {
      if (copyType == Type.FULL) {
        SnapshotCopy snapshotCp =
            new SnapshotCopy(handler, handler.getBackupContext().getTableBySnapshot(options[1]));
        LOG.debug("Doing SNAPSHOT_COPY");
        // Make a new instance of conf to be used by the snapshot copy class.
        snapshotCp.setConf(new Configuration(conf));
        res = snapshotCp.run(options);
      } else if (copyType == Type.INCREMENTAL) {
        LOG.debug("Doing COPY_TYPE_DISTCP");
        setSubTaskPercntgInWholeTask(1f);

        BackupDistCp distcp = new BackupDistCp(new Configuration(conf), null, handler);
        // Handle a special case where the source file is a single file.
        // In this case, distcp will not create the target dir. It just take the
        // target as a file name and copy source file to the target (as a file name).
        // We need to create the target dir before run distcp.
        LOG.debug("DistCp options: " + Arrays.toString(options));
        if (options.length == 2) {
          Path dest = new Path(options[1]);
          FileSystem destfs = dest.getFileSystem(conf);
          if (!destfs.exists(dest)) {
            destfs.mkdirs(dest);
          }
        }

        res = distcp.run(options);
      }
      return res;

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
