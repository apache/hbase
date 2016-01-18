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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * Multithreaded implementation for @link org.apache.hbase.mapreduce.TableMapper
 * <p>
 * It can be used instead when the Map operation is not CPU
 * bound in order to improve throughput.
 * <p>
 * Mapper implementations using this MapRunnable must be thread-safe.
 * <p>
 * The Map-Reduce job has to be configured with the mapper to use via
 * {@link #setMapperClass} and the number of thread the thread-pool can use with the
 * {@link #getNumberOfThreads} method. The default value is 10 threads.
 * <p>
 */

public class MultithreadedTableMapper<K2, V2> extends TableMapper<K2, V2> {
  private static final Log LOG = LogFactory.getLog(MultithreadedTableMapper.class);
  private Class<? extends Mapper<ImmutableBytesWritable, Result,K2,V2>> mapClass;
  private Context outer;
  private ExecutorService executor;
  public static final String NUMBER_OF_THREADS = "hbase.mapreduce.multithreadedmapper.threads";
  public static final String MAPPER_CLASS = "hbase.mapreduce.multithreadedmapper.mapclass";

  /**
   * The number of threads in the thread pool that will run the map function.
   * @param job the job
   * @return the number of threads
   */
  public static int getNumberOfThreads(JobContext job) {
    return job.getConfiguration().
        getInt(NUMBER_OF_THREADS, 10);
  }

  /**
   * Set the number of threads in the pool for running maps.
   * @param job the job to modify
   * @param threads the new number of threads
   */
  public static void setNumberOfThreads(Job job, int threads) {
    job.getConfiguration().setInt(NUMBER_OF_THREADS,
        threads);
  }

  /**
   * Get the application's mapper class.
   * @param <K2> the map's output key type
   * @param <V2> the map's output value type
   * @param job the job
   * @return the mapper class to run
   */
  @SuppressWarnings("unchecked")
  public static <K2,V2>
  Class<Mapper<ImmutableBytesWritable, Result,K2,V2>> getMapperClass(JobContext job) {
    return (Class<Mapper<ImmutableBytesWritable, Result,K2,V2>>)
        job.getConfiguration().getClass( MAPPER_CLASS,
            Mapper.class);
  }

  /**
   * Set the application's mapper class.
   * @param <K2> the map output key type
   * @param <V2> the map output value type
   * @param job the job to modify
   * @param cls the class to use as the mapper
   */
  public static <K2,V2>
  void setMapperClass(Job job,
      Class<? extends Mapper<ImmutableBytesWritable, Result,K2,V2>> cls) {
    if (MultithreadedTableMapper.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("Can't have recursive " +
          "MultithreadedTableMapper instances.");
    }
    job.getConfiguration().setClass(MAPPER_CLASS,
        cls, Mapper.class);
  }

  /**
   * Run the application's maps using a thread pool.
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    outer = context;
    int numberOfThreads = getNumberOfThreads(context);
    mapClass = getMapperClass(context);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring multithread runner to use " + numberOfThreads +
          " threads");
    }
    executor = Executors.newFixedThreadPool(numberOfThreads);
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = new MapRunner(context);
      executor.execute(thread);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
      // wait till all the threads are done
      Thread.sleep(1000);
    }
  }

  private class SubMapRecordReader
  extends RecordReader<ImmutableBytesWritable, Result> {
    private ImmutableBytesWritable key;
    private Result value;
    private Configuration conf;

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit split,
        TaskAttemptContext context
        ) throws IOException, InterruptedException {
      conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      synchronized (outer) {
        if (!outer.nextKeyValue()) {
          return false;
        }
        key = ReflectionUtils.copy(outer.getConfiguration(),
            outer.getCurrentKey(), key);
        value = ReflectionUtils.copy(conf, outer.getCurrentValue(), value);
        return true;
      }
    }

    public ImmutableBytesWritable getCurrentKey() {
      return key;
    }

    @Override
    public Result getCurrentValue() {
      return value;
    }
  }

  private class SubMapRecordWriter extends RecordWriter<K2,V2> {

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
    }

    @Override
    public void write(K2 key, V2 value) throws IOException,
    InterruptedException {
      synchronized (outer) {
        outer.write(key, value);
      }
    }
  }

  private class SubMapStatusReporter extends StatusReporter {

    @Override
    public Counter getCounter(Enum<?> name) {
      return outer.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return outer.getCounter(group, name);
    }

    @Override
    public void progress() {
      outer.progress();
    }

    @Override
    public void setStatus(String status) {
      outer.setStatus(status);
    }

    public float getProgress() {
      return 0;
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
      justification="Don't understand why FB is complaining about this one. We do throw exception")
  private class MapRunner implements Runnable {
    private Mapper<ImmutableBytesWritable, Result, K2,V2> mapper;
    private Context subcontext;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    MapRunner(Context context) throws IOException, InterruptedException {
      mapper = ReflectionUtils.newInstance(mapClass,
          context.getConfiguration());
      try {
        Constructor c = context.getClass().getConstructor(
          Mapper.class,
          Configuration.class,
          TaskAttemptID.class,
          RecordReader.class,
          RecordWriter.class,
          OutputCommitter.class,
          StatusReporter.class,
          InputSplit.class);
        c.setAccessible(true);
        subcontext = (Context) c.newInstance(
          mapper,
          outer.getConfiguration(), 
          outer.getTaskAttemptID(),
          new SubMapRecordReader(),
          new SubMapRecordWriter(),
          context.getOutputCommitter(),
          new SubMapStatusReporter(),
          outer.getInputSplit());
      } catch (Exception e) {
        try {
          Constructor c = Class.forName("org.apache.hadoop.mapreduce.task.MapContextImpl").getConstructor(
            Configuration.class,
            TaskAttemptID.class,
            RecordReader.class,
            RecordWriter.class,
            OutputCommitter.class,
            StatusReporter.class,
            InputSplit.class);
          c.setAccessible(true);
          MapContext mc = (MapContext) c.newInstance(
            outer.getConfiguration(), 
            outer.getTaskAttemptID(),
            new SubMapRecordReader(),
            new SubMapRecordWriter(),
            context.getOutputCommitter(),
            new SubMapStatusReporter(),
            outer.getInputSplit());
          Class<?> wrappedMapperClass = Class.forName("org.apache.hadoop.mapreduce.lib.map.WrappedMapper");
          Method getMapContext = wrappedMapperClass.getMethod("getMapContext", MapContext.class);
          subcontext = (Context) getMapContext.invoke(wrappedMapperClass.newInstance(), mc);
        } catch (Exception ee) { // FindBugs: REC_CATCH_EXCEPTION
          // rethrow as IOE
          throw new IOException(e);
        }
      }
    }

    @Override
    public void run() {
      try {
        mapper.run(subcontext);
      } catch (Throwable ie) {
        LOG.error("Problem in running map.", ie);
      }
    }
  }
}
