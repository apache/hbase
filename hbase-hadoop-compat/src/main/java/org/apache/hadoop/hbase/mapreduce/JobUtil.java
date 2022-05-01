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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to interact with a job.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class JobUtil {
  private static final Logger LOG = LoggerFactory.getLogger(JobUtil.class);

  protected JobUtil() {
    super();
  }

  /**
   * Initializes the staging directory and returns the path.
   * @param conf system configuration
   * @return staging directory path
   * @throws IOException          if the ownership on the staging directory is not as expected
   * @throws InterruptedException if the thread getting the staging directory is interrupted
   */
  public static Path getStagingDir(Configuration conf) throws IOException, InterruptedException {
    return JobSubmissionFiles.getStagingDir(new Cluster(conf), conf);
  }

  /**
   * Initializes the staging directory and returns the qualified path.
   * @param conf conf system configuration
   * @return qualified staging directory path
   * @throws IOException          if the ownership on the staging directory is not as expected
   * @throws InterruptedException if the thread getting the staging directory is interrupted
   */
  public static Path getQualifiedStagingDir(Configuration conf)
    throws IOException, InterruptedException {
    Cluster cluster = new Cluster(conf);
    Path stagingDir = JobSubmissionFiles.getStagingDir(cluster, conf);
    return cluster.getFileSystem().makeQualified(stagingDir);
  }

}
