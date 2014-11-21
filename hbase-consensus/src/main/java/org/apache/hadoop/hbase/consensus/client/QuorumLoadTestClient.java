package org.apache.hadoop.hbase.consensus.client;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumLoadTestClient {
  private static final Logger LOG = LoggerFactory.getLogger(
          QuorumLoadTestClient.class);

  private static int status_interval = 1000;
  private static int progress_interval = 100;
  private static int maxBatchSize = 5;

  private static QuorumClient client;
  private final static ExecutorService EXEC_SERVICE_FOR_THRIFT_CLIENTS =
      Executors.newFixedThreadPool(5, new DaemonThreadFactory("QuorumClient-"));

  private static int[] payloadSize = {
    512, 1 * 1024, 4 * 1024, 16 * 1024, 64 * 1024,
    128 * 1024, 256 * 1024, 512 * 1024,
  };

  public static void loadTest() {
    // generate the test data
    int testDataIndex = 0;
    List<WALEdit> testData = new ArrayList<>();

    while(true) {

      for (int batchSize = 1; batchSize <= maxBatchSize; batchSize++) {
        testData.clear();
        String payloadSizeName = FileUtils.byteCountToDisplaySize(payloadSize[testDataIndex]);

        for (int j = 0; j < batchSize; j++) {
          KeyValue kv = KeyValue.generateKeyValue(
            payloadSizeName,payloadSize[testDataIndex]);
          testData.add(new WALEdit(Arrays.asList(kv)));
        }

        long start = System.nanoTime();
        for (int s = 0; s < status_interval; s++) {
          try {
            client.replicateCommits(testData);
            if (s % progress_interval == 0) {
              System.out.print("+");
            }
          } catch (Exception e) {
            System.out.println("Unexpected exception " + e);
            e.printStackTrace();
          }
        }

        float timeInMicroSec =  (System.nanoTime() - start) / (float) 1000;
        float avgLatency = timeInMicroSec / status_interval;
        float size = payloadSize[testDataIndex] * batchSize;
        float avgThroughput = size / (timeInMicroSec / 1000/ 1000) ;

        String latency = new DecimalFormat("##.##").format(avgLatency);
        String throughput = FileUtils.byteCountToDisplaySize((long)avgThroughput) + "/sec";

        System.out.println("[kv_size: " + payloadSizeName +
          ", batch_size: " + batchSize +
          ", avg_latency: " + latency + " microsec" +
          ", throughput: " + throughput + " ]");
      }

      if (++testDataIndex == payloadSize.length) {
        testDataIndex = 0;
      }

    }
  }

  public static void main(String[] args) throws IOException {

    Options opt = new Options();
    opt.addOption("region", true, "the region id for the load test");
    opt.addOption("servers", true, "A list of quorum server delimited by ,");

    opt.addOption("max_batch_size", true, "The max number of kvs in each transaction");
    opt.addOption("status_interval", true, "To print the status at very $status_interval " +
      "transactions");
    opt.addOption("progress_interval", true,
      "To print the progress at very $progress_interval transactions");

    String regionId = null;
    String serverList;
    String servers[] = null;

    try {
      CommandLine cmd = new GnuParser().parse(opt, args);

      if (cmd.hasOption("region")) {
        regionId = cmd.getOptionValue("region");
      }

      if (cmd.hasOption("servers")) {
        serverList = cmd.getOptionValue("servers");
        servers = serverList.split(",");
      }

      if (cmd.hasOption("max_batch_size")) {
        maxBatchSize = Integer.valueOf(cmd.getOptionValue("max_batch_size"));
      }

      if (cmd.hasOption("status_interval")) {
        status_interval = Integer.valueOf(cmd.getOptionValue("status_interval"));
      }

      if (cmd.hasOption("progress_interval")) {
        progress_interval = Integer.valueOf(cmd.getOptionValue("progress_interval"));
      }

      if (regionId == null || regionId.isEmpty() || servers == null || servers.length == 0) {
        LOG.error("Wrong args !");
        printHelp(opt);
        System.exit(-1);
      }

      System.out.println("*******************");
      System.out.println("region: " + regionId);

      Configuration conf = HBaseConfiguration.create();
      client = new QuorumClient(regionId, conf, EXEC_SERVICE_FOR_THRIFT_CLIENTS);

      System.out.println("Initialized the quorum client");
      System.out.println("*******************");
    } catch (Exception e) {
      e.printStackTrace();
      printHelp(opt);
      System.exit(-1);
    }

    System.out.println("Starting client for load testing");
    loadTest();
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
      "QuorumLoadTestClient -region regionID -servers h1:port,h2:port,h3:port..." +
        "[-status_interval interval] [-progress_interval interval]", opt);
  }
}
