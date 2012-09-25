package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LoadTest extends Configured implements Tool {

  public static final String NAME = "loadtest";

  private static final int DEFAULT_JMX_PORT = 8991;

  private static final String WORKLOADGENERATOR =
      "hbase.loadtester.workloadgenerator";

  private static final String WORKLOADGENERATOR_ARGS =
      "hbase.loadtester.generator.args";

  private static final String TABLENAME = "hbase.loadtester.tablename";

  private static final String DEFAULT_TABLENAME = "test1";

  private static final String DATAGENERATOR =
      "hbase.loadtester.datagenerator";

  private static final String DATAGENERATOR_ARGS =
      "hbase.loadtester.datagenerator.args";

  private static final String DEFAULT_DATAGENERATOR =
      RandomDataGenerator.class.getName();
  
  public static final double DEFAULT_PROFILING_FRACTION = 0.001;

  // Since all tasks share the same jmx port, some tasks might fail since
  // they might run on the same machine and try to bind to the same jmx port.
  // Alleviating this situation by retrying tasks as long as we can.
  public static final int MAX_REDUCE_TASK_ATTEMPTS = 1000000;

  public static class Map
      extends Mapper<LongWritable, Text, LongWritable, Text> {

    // The mapper should only be executed once. Ensure that each mapper only
    // sends workloads once by flagging the first pass of each mapper. The
    // driver will be responsible for ensuring that there is only one mapper.
    private boolean firstPass;

    public Map() {
      this.firstPass = true;
    }

    public void map(LongWritable key, Text value,
        Mapper<LongWritable, Text, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {
      if (firstPass) {
        // Determine from the job configuration which workloads to generate.
        String className = context.getConfiguration().get(WORKLOADGENERATOR);
        Workload.Generator workloadGenerator =
            Workload.Generator.newInstance(className);

        // Determine from the job configuration the name of the HBase table to
        // use, and create that table according to the requirements of the
        // workload generator.
        String tableName = context.getConfiguration().get(TABLENAME,
            DEFAULT_TABLENAME);
        workloadGenerator.setupTable(configurationFromZooKeeper(
            context.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM)),
            tableName);

        // Determine from the job configuration the arguments for the workload
        // generator.
        String generatorArgs =
            context.getConfiguration().get(WORKLOADGENERATOR_ARGS);

        // Generate the workloads for each of the reducer tasks.
        List<List<Workload>> workloads =
            workloadGenerator.generateWorkloads(context.getNumReduceTasks(),
                generatorArgs);

        // Serialize the workloads assigned to each reducer, and write them out.
        // Multiple workloads for the same client may share common objects, so
        // they must be serialized together into a single blob.
        for (int i = 0; i < workloads.size(); i++) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream out = new ObjectOutputStream(baos);
          out.writeObject(workloads.get(i));
          out.close();

          // The key is used to partition the workloads to different reducers.
          context.write(new LongWritable(i), new Text(baos.toByteArray()));
        }
        firstPass = false;
      }
    }
  }

  public static class Reduce
      extends Reducer<LongWritable, Text, LongWritable, Text> {

    public void reduce(LongWritable key, Iterable<Text> values,
        Reducer<LongWritable, Text, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {
      HBaseConfiguration conf = configurationFromZooKeeper(
          context.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));

      Operation.setConfAndTableName(conf, context.getConfiguration().get(
          TABLENAME, DEFAULT_TABLENAME).getBytes());

      DataGenerator dataGenerator = DataGenerator.newInstance(
          context.getConfiguration().get(DATAGENERATOR, DEFAULT_DATAGENERATOR),
          context.getConfiguration().get(DATAGENERATOR_ARGS));

      StatsCollector statsCollector = StatsCollector.getInstance();

      List<Workload.Executor> executors = new ArrayList<Workload.Executor>();

      for (Text value : values) {
        try {
          // Deserialize the workloads from the input.
          ObjectInputStream in = new ObjectInputStream(
              new ByteArrayInputStream(value.getBytes()));

          @SuppressWarnings("unchecked")
          List<Workload> workloads = (List<Workload>)in.readObject();
          for (Workload workload : workloads) {
            // Run each workload in its own executor, with dedicated thread
            // pools per workload.
            Workload.Executor executor =
                new Workload.Executor(workload, dataGenerator);
            executors.add(executor);
            executor.start();

            // Initialize the stats for each of the types of operations used
            // by this workload.
            statsCollector.initializeTypes(workload.getOperationTypes());
          }
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
      }

      // Start the reporters for the initialized types of operations.
      statsCollector.startMBeanReporters();
      statsCollector.startConsoleReporters();
      statsCollector.startMapReduceCounterReporter(context);

      // Wait for the executors to finish. They should never actually finish,
      // but if the inputs are exhausted then the reducer will terminate and
      // the job will finish prematurely.
      for (Workload.Executor executor : executors) {
        executor.waitForFinish();
      }
    }
  }

  public static class Partition extends Partitioner<LongWritable, Text> {
    public int getPartition(LongWritable key, Text value, int numPartitions) {
      // Keys should be uniformly distributed across reducers.
      return (int)(key.get() % numPartitions);
    }
  }

  @SuppressWarnings("deprecation")
  public static HBaseConfiguration configurationFromZooKeeper(String zkQuorum) {
    Configuration conf = new Configuration();
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    // This constructor is deprecated, but there seems to be no other way to get
    // a HBaseConfiguration instance, and casting from Configuration fails at
    // runtime.
    return new HBaseConfiguration(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();

    String[] otherArgs =
        new GenericOptionsParser(conf, args).getRemainingArgs();
    Options options = new Options();
    options.addOption("zk", true, "ZooKeeper quorum");
    options.addOption("tasks", true, "Number of client tasks");
    options.addOption("table", true, "HBase table name");
    options.addOption("wl", true, "WorkloadGenerator class name");
    options.addOption("wlargs", true, "WorkloadGenerator arguments");
    options.addOption("data", true, "DataGenerator class name");
    options.addOption("dataargs", true, "DataGenerator arguments");
    options.addOption("jmx", true, "Port number for remote JMX");

    CommandLine cmd = new GnuParser().parse(options, otherArgs);
    if (cmd.hasOption("zk")) {
      String quorum = "";
      for (String node : cmd.getOptionValues("zk")) {
        quorum += node + ",";
      }
      conf.set(HConstants.ZOOKEEPER_QUORUM,
          quorum.substring(0, quorum.length() - 1));
    } else {
      System.err.println("ZooKeeper quorum must be specified");
      System.exit(1);
    }

    int numTasks = 1;
    if (cmd.hasOption("tasks")) {
      numTasks = Integer.parseInt(cmd.getOptionValue("tasks"));
      if (numTasks == 0) {
        System.err.println("The number of client tasks must be positive");
        System.exit(1);
      }
    } else {
      System.err.println("The number of client tasks must be specified");
      System.exit(1);
    }

    if (cmd.hasOption("table")) {
      conf.set(TABLENAME, cmd.getOptionValue("table"));
    }

    if (cmd.hasOption("wl")) {
      conf.set(WORKLOADGENERATOR, cmd.getOptionValue("wl"));
    } else {
      System.err.println("No workloads specified");
      System.exit(1);
    }

    if (cmd.hasOption("wlargs")) {
      conf.set(WORKLOADGENERATOR_ARGS, cmd.getOptionValue("wlargs"));
    }

    if (cmd.hasOption("data")) {
      conf.set(DATAGENERATOR, cmd.getOptionValue("data"));
    }

    if (cmd.hasOption("dataargs")) {
      conf.set(DATAGENERATOR_ARGS, cmd.getOptionValue("dataargs"));
    }

    int jmxPort = DEFAULT_JMX_PORT;
    if (cmd.hasOption("jmx")) {
      jmxPort = Integer.parseInt(cmd.getOptionValue("jmx"));
    }
    conf.set("mapred.child.java.opts",
        "-Dcom.sun.management.jmxremote.authenticate=false" +
        " -Dcom.sun.management.jmxremote.ssl=false" +
        " -Dcom.sun.management.jmxremote.port=" + jmxPort);
    conf.setInt("mapred.reduce.max.attempts", MAX_REDUCE_TASK_ATTEMPTS);

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    String currentDate = dateFormat.format(new Date());

    // Create a a single input file with a single key-value pair. The contents
    // of the input file are irrelevant as the mapper does not care. The
    // important constraint is that there is only one input file. The input and
    // output paths are date-specific to avoid conflict, though no output is
    // expected.
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path("/tmp/input/" + currentDate);
    Path outputPath = new Path("/tmp/output/" + currentDate);
    OutputStream out = fs.create(inputPath);
    out.write("0\t0".getBytes());
    out.close();

    Job job = new Job(conf, "LoadTester_" + currentDate + "_" +
        Class.forName(conf.get(WORKLOADGENERATOR)).getSimpleName());
    job.setJarByClass(LoadTest.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setPartitionerClass(Partition.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(numTasks);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new LoadTest(), args);
    System.exit(result);
  }
}
