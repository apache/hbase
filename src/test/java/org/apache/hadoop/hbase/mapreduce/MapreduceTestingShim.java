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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

/**
 * This class provides shims for HBase to interact with the Hadoop 1.0.x and the
 * Hadoop 0.23.x series.
 * 
 * NOTE: No testing done against 0.22.x, or 0.21.x.
 */
abstract public class MapreduceTestingShim {
  private static MapreduceTestingShim instance;

  static {
    try {
      // This class exists in hadoop 0.22+ but not in Hadoop 20.x/1.x
      Class c = Class
          .forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      instance = new MapreduceV2Shim();
    } catch (Exception e) {
      instance = new MapreduceV1Shim();
    }
  }

  abstract public JobContext newJobContext(Configuration jobConf)
      throws IOException;

  public static JobContext createJobContext(Configuration jobConf)
      throws IOException {
    return instance.newJobContext(jobConf);
  }

  private static class MapreduceV1Shim extends MapreduceTestingShim {
    public JobContext newJobContext(Configuration jobConf) throws IOException {
      // Implementing:
      // return new JobContext(jobConf, new JobID());
      JobID jobId = new JobID();
      Constructor<JobContext> c;
      try {
        c = JobContext.class.getConstructor(Configuration.class, JobID.class);
        return c.newInstance(jobConf, jobId);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to instantiate new JobContext(jobConf, new JobID())", e);
      }
    }
  };

  private static class MapreduceV2Shim extends MapreduceTestingShim {
    public JobContext newJobContext(Configuration jobConf) {
      // Implementing:
      // return Job.getInstance(jobConf);
      try {
        Method m = Job.class.getMethod("getInstance", Configuration.class);
        return (JobContext) m.invoke(null, jobConf); // static method, then arg
      } catch (Exception e) {
        e.printStackTrace();
        throw new IllegalStateException(
            "Failed to return from Job.getInstance(jobConf)");
      }
    }
  };

}
