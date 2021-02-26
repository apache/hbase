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
package org.apache.hadoop.hbase.tool;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
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
  private static Class[] emptyParam = new Class[] {};

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

  abstract public Job newJob(Configuration conf) throws IOException;

  abstract public JobConf obtainJobConf(MiniMRCluster cluster);

  abstract public String obtainMROutputDirProp();

  public static JobContext createJobContext(Configuration jobConf)
      throws IOException {
    return instance.newJobContext(jobConf);
  }

  public static JobConf getJobConf(MiniMRCluster cluster) {
    return instance.obtainJobConf(cluster);
  }

  public static Job createJob(Configuration conf) throws IOException {
    return instance.newJob(conf);
  }

  public static String getMROutputDirProp() {
    return instance.obtainMROutputDirProp();
  }

  private static class MapreduceV1Shim extends MapreduceTestingShim {
    @Override
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

    @Override
    public Job newJob(Configuration conf) throws IOException {
      // Implementing:
      // return new Job(conf);
      Constructor<Job> c;
      try {
        c = Job.class.getConstructor(Configuration.class);
        return c.newInstance(conf);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to instantiate new Job(conf)", e);
      }
    }

    @Override
    public JobConf obtainJobConf(MiniMRCluster cluster) {
      if (cluster == null) return null;
      try {
        Object runner = cluster.getJobTrackerRunner();
        Method meth = runner.getClass().getDeclaredMethod("getJobTracker", emptyParam);
        Object tracker = meth.invoke(runner, new Object []{});
        Method m = tracker.getClass().getDeclaredMethod("getConf", emptyParam);
        return (JobConf) m.invoke(tracker, new Object []{});
      } catch (NoSuchMethodException nsme) {
        return null;
      } catch (InvocationTargetException ite) {
        return null;
      } catch (IllegalAccessException iae) {
        return null;
      }
    }

    @Override
    public String obtainMROutputDirProp() {
      return "mapred.output.dir";
    }
  };

  private static class MapreduceV2Shim extends MapreduceTestingShim {
    @Override
    public JobContext newJobContext(Configuration jobConf) {
      return newJob(jobConf);
    }

    @Override
    public Job newJob(Configuration jobConf) {
      // Implementing:
      // return Job.getInstance(jobConf);
      try {
        Method m = Job.class.getMethod("getInstance", Configuration.class);
        return (Job) m.invoke(null, jobConf); // static method, then arg
      } catch (Exception e) {
        e.printStackTrace();
        throw new IllegalStateException(
            "Failed to return from Job.getInstance(jobConf)");
      }
    }

    @Override
    public JobConf obtainJobConf(MiniMRCluster cluster) {
      try {
        Method meth = MiniMRCluster.class.getMethod("getJobTrackerConf", emptyParam);
        return (JobConf) meth.invoke(cluster, new Object []{});
      } catch (NoSuchMethodException nsme) {
        return null;
      } catch (InvocationTargetException ite) {
        return null;
      } catch (IllegalAccessException iae) {
        return null;
      }
    }

    @Override
    public String obtainMROutputDirProp() {
      // This is a copy of o.a.h.mapreduce.lib.output.FileOutputFormat.OUTDIR
      // from Hadoop 0.23.x.  If we use the source directly we break the hadoop 1.x compile.
      return "mapreduce.output.fileoutputformat.outputdir";
    }
  };

}
