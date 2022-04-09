/**
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
package org.apache.hadoop.hbase.mob.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Scans a given table + CF for all mob reference cells to get the list of backing mob files. For
 * each referenced file we attempt to verify that said file is on the FileSystem in a place that the
 * MOB system will look when attempting to resolve the actual value.
 * <p/>
 * The job includes counters that can help provide a rough sketch of the mob data.
 *
 * <pre>
 * Map-Reduce Framework
 *         Map input records=10000
 * ...
 *         Reduce output records=99
 * ...
 * CELLS PER ROW
 *         Number of rows with 1s of cells per row=10000
 * MOB
 *         NUM_CELLS=52364
 * PROBLEM
 *         Affected rows=338
 *         Problem MOB files=2
 * ROWS WITH PROBLEMS PER FILE
 *         Number of HFiles with 100s of affected rows=2
 * SIZES OF CELLS
 *         Number of cells with size in the 10,000s of bytes=627
 *         Number of cells with size in the 100,000s of bytes=51392
 *         Number of cells with size in the 1,000,000s of bytes=345
 * SIZES OF ROWS
 *         Number of rows with total size in the 100,000s of bytes=6838
 *         Number of rows with total size in the 1,000,000s of bytes=3162
 * </pre>
 * <ol>
 * <li>Map-Reduce Framework:Map input records - the number of rows with mob references</li>
 * <li>Map-Reduce Framework:Reduce output records - the number of unique hfiles referenced</li>
 * <li>MOB:NUM_CELLS - the total number of mob reference cells</li>
 * <li>PROBLEM:Affected rows - the number of rows that reference hfiles with an issue</li>
 * <li>PROBLEM:Problem MOB files - the number of unique hfiles that have an issue</li>
 * <li>CELLS PER ROW: - this counter group gives a histogram of the order of magnitude of the number
 * of cells in a given row by grouping by the number of digits used in each count. This allows us to
 * see more about the distribution of cells than what we can determine with just the cell count and
 * the row count. In this particular example we can see that all of our rows have somewhere between
 * 1 - 9 cells.</li>
 * <li>ROWS WITH PROBLEMS PER FILE: - this counter group gives a histogram of the order of magnitude
 * of the number of rows in each of the hfiles with a problem. e.g. in the example there are 2
 * hfiles and they each have the same order of magnitude number of rows, specifically between 100
 * and 999.</li>
 * <li>SIZES OF CELLS: - this counter group gives a histogram of the order of magnitude of the size
 * of mob values according to our reference cells. e.g. in the example above we have cell sizes that
 * are all between 10,000 bytes and 9,999,999 bytes. From this histogram we can also see that _most_
 * cells are 100,000 - 999,000 bytes and the smaller and bigger ones are outliers making up less
 * than 2% of mob cells.</li>
 * <li>SIZES OF ROWS: - this counter group gives a histogram of the order of magnitude of the size
 * of mob values across each row according to our reference cells. In the example above we have rows
 * that are are between 100,000 bytes and 9,999,999 bytes. We can also see that about 2/3rd of our
 * rows are 100,000 - 999,999 bytes.</li>
 * </ol>
 * Generates a report that gives one file status per line, with tabs dividing fields.
 *
 * <pre>
 * RESULT OF LOOKUP	FILE REF	comma seperated, base64 encoded rows when there's a problem
 * </pre>
 *
 * e.g.
 *
 * <pre>
 * MOB DIR	09c576e28a65ed2ead0004d192ffaa382019110184b30a1c7e034573bf8580aef8393402
 * MISSING FILE    28e252d7f013973174750d483d358fa020191101f73536e7133f4cd3ab1065edf588d509        MmJiMjMyYzBiMTNjNzc0OTY1ZWY4NTU4ZjBmYmQ2MTUtNTIz,MmEzOGE0YTkzMTZjNDllNWE4MzM1MTdjNDVkMzEwNzAtODg=
 * </pre>
 *
 * Possible results are listed; the first three indicate things are working properly.
 * <ol>
 * <li>MOB DIR - the reference is in the normal MOB area for the given table and CF</li>
 * <li>HLINK TO ARCHIVE FOR SAME TABLE - the reference is present in the archive area for this table
 * and CF</li>
 * <li>HLINK TO ARCHIVE FOR OTHER TABLE - the reference is present in a different table and CF,
 * either in the MOB or archive areas (e.g. from a snapshot restore or clone)</li>
 * <li>ARCHIVE WITH HLINK BUT NOT FROM OUR TABLE - the reference is currently present in the archive
 * area for this table and CF, but it is kept there because a _different_ table has a reference to
 * it (e.g. from a snapshot clone). If these other tables are removed then the file will likely be
 * deleted unless there is a snapshot also referencing it.</li>
 * <li>ARCHIVE BUT NO HLINKS - the reference is currently present in the archive for this table and
 * CF, but there are no references present to prevent its removal. Unless it is newer than the
 * general TTL (default 5 minutes) or referenced in a snapshot it will be subject to cleaning.</li>
 * <li>ARCHIVE BUT FAILURE WHILE CHECKING HLINKS - Check the job logs to see why things failed while
 * looking for why this file is being kept around.</li>
 * <li>MISSING FILE - We couldn't find the reference on the FileSystem. Either there is dataloss due
 * to a bug in the MOB storage system or the MOB storage is damaged but in an edge case that allows
 * it to work for now. You can verify which by doing a raw reference scan to get the referenced
 * hfile and check the underlying filesystem. See the ref guide section on mob for details.</li>
 * <li>HLINK BUT POINT TO MISSING FILE - There is a pointer in our mob area for this table and CF to
 * a file elsewhere on the FileSystem, however the file it points to no longer exists.</li>
 * <li>MISSING FILE BUT FAILURE WHILE CHECKING HLINKS - We could not find the referenced file,
 * however you should check the job logs to see why we couldn't check to see if there is a pointer
 * to the referenced file in our archive or another table's archive or mob area.</li>
 * </ol>
 */
@InterfaceAudience.Private
public class MobRefReporter extends Configured implements Tool {
  private static Logger LOG = LoggerFactory.getLogger(MobRefReporter.class);
  public static final String NAME = "mobrefs";
  static final String REPORT_JOB_ID = "mob.report.job.id";
  static final String REPORT_START_DATETIME = "mob.report.job.start";

  public static class MobRefMapper extends TableMapper<Text, ImmutableBytesWritable> {
    @Override
    public void map(ImmutableBytesWritable r, Result columns, Context context) throws IOException,
        InterruptedException {
      if (columns == null) {
        return;
      }
      Cell[] cells = columns.rawCells();
      if (cells == null || cells.length == 0) {
        return;
      }
      Set<String> files = new HashSet<>();
      long count = 0;
      long size = 0;
      for (Cell c : cells) {
        if (MobUtils.hasValidMobRefCellValue(c)) {
          // TODO confirm there aren't tags
          String fileName = MobUtils.getMobFileName(c);
          if (!files.contains(fileName)) {
            context.write(new Text(fileName), r);
            files.add(fileName);
          }
          final int cellsize = MobUtils.getMobValueLength(c);
          context.getCounter("SIZES OF CELLS", "Number of cells with size in the " +
              log10GroupedString(cellsize) + "s of bytes").increment(1L);
          size += cellsize;
          count++;
        } else {
          LOG.debug("cell is not a mob ref, even though we asked for only refs. cell={}", c);
        }
      }
      context.getCounter("CELLS PER ROW", "Number of rows with " + log10GroupedString(count) +
          "s of cells per row").increment(1L);
      context.getCounter("SIZES OF ROWS", "Number of rows with total size in the " +
          log10GroupedString(size) + "s of bytes").increment(1L);
      context.getCounter("MOB","NUM_CELLS").increment(count);
    }
  }

  public static class MobRefReducer extends
      Reducer<Text, ImmutableBytesWritable, Text, Text> {

    TableName table;
    String mobRegion;
    Path mob;
    Path archive;
    String seperator;

    /* Results that mean things are fine */
    final Text OK_MOB_DIR = new Text("MOB DIR");
    final Text OK_HLINK_RESTORE = new Text("HLINK TO ARCHIVE FOR SAME TABLE");
    final Text OK_HLINK_CLONE = new Text("HLINK TO ARCHIVE FOR OTHER TABLE");
    /* Results that mean something is incorrect */
    final Text INCONSISTENT_ARCHIVE_BAD_LINK =
        new Text("ARCHIVE WITH HLINK BUT NOT FROM OUR TABLE");
    final Text INCONSISTENT_ARCHIVE_STALE = new Text("ARCHIVE BUT NO HLINKS");
    final Text INCONSISTENT_ARCHIVE_IOE = new Text("ARCHIVE BUT FAILURE WHILE CHECKING HLINKS");
    /* Results that mean data is probably already gone */
    final Text DATALOSS_MISSING = new Text("MISSING FILE");
    final Text DATALOSS_HLINK_DANGLING = new Text("HLINK BUT POINTS TO MISSING FILE");
    final Text DATALOSS_MISSING_IOE = new Text("MISSING FILE BUT FAILURE WHILE CHECKING HLINKS");
    final Base64.Encoder base64 = Base64.getEncoder();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      final Configuration conf = context.getConfiguration();
      final String tableName = conf.get(TableInputFormat.INPUT_TABLE);
      if (null == tableName) {
        throw new IOException("Job configuration did not include table.");
      }
      table = TableName.valueOf(tableName);
      mobRegion = MobUtils.getMobRegionInfo(table).getEncodedName();
      final String family = conf.get(TableInputFormat.SCAN_COLUMN_FAMILY);
      if (null == family) {
        throw new IOException("Job configuration did not include column family");
      }
      mob = MobUtils.getMobFamilyPath(conf, table, family);
      LOG.info("Using active mob area '{}'", mob);
      archive = HFileArchiveUtil.getStoreArchivePath(conf, table,
          MobUtils.getMobRegionInfo(table).getEncodedName(), family);
      LOG.info("Using archive mob area '{}'", archive);
      seperator = conf.get(TextOutputFormat.SEPERATOR, "\t");
    }

    @Override
    public void reduce(Text key, Iterable<ImmutableBytesWritable> rows, Context context)
        throws IOException, InterruptedException {
      final Configuration conf = context.getConfiguration();
      final String file = key.toString();
      // active mob area
      if (mob.getFileSystem(conf).exists(new Path(mob, file))) {
        LOG.debug("Found file '{}' in mob area", file);
        context.write(OK_MOB_DIR, key);
      // archive area - is there an hlink back reference (from a snapshot from same table)
      } else if (archive.getFileSystem(conf).exists(new Path(archive, file))) {

        Path backRefDir = HFileLink.getBackReferencesDir(archive, file);
        try {
          FileStatus[] backRefs = CommonFSUtils.listStatus(archive.getFileSystem(conf), backRefDir);
          if (backRefs != null) {
            boolean found = false;
            for (FileStatus backRef : backRefs) {
              Pair<TableName, String> refParts = HFileLink.parseBackReferenceName(
                  backRef.getPath().getName());
              if (table.equals(refParts.getFirst()) && mobRegion.equals(refParts.getSecond())) {
                Path hlinkPath = HFileLink.getHFileFromBackReference(MobUtils.getMobHome(conf),
                    backRef.getPath());
                if (hlinkPath.getFileSystem(conf).exists(hlinkPath)) {
                  found = true;
                } else {
                  LOG.warn("Found file '{}' in archive area with a back reference to the mob area "
                      + "for our table, but the mob area does not have a corresponding hfilelink.",
                      file);
                }
              }
            }
            if (found) {
              LOG.debug("Found file '{}' in archive area. has proper hlink back references to "
                  + "suggest it is from a restored snapshot for this table.", file);
              context.write(OK_HLINK_RESTORE, key);
            } else {
              LOG.warn("Found file '{}' in archive area, but the hlink back references do not "
                  + "properly point to the mob area for our table.", file);
              context.write(INCONSISTENT_ARCHIVE_BAD_LINK, encodeRows(context, key, rows));
            }
          } else {
            LOG.warn("Found file '{}' in archive area, but there are no hlinks pointing to it. Not "
                + "yet used snapshot or an error.", file);
            context.write(INCONSISTENT_ARCHIVE_STALE, encodeRows(context, key, rows));
          }
        } catch (IOException e) {
          LOG.warn("Found file '{}' in archive area, but got an error while checking "
              + "on back references.", file, e);
          context.write(INCONSISTENT_ARCHIVE_IOE, encodeRows(context, key, rows));
        }

      } else {
        // check for an hlink in the active mob area (from a snapshot of a different table)
        try {
          /**
           * we are doing this ourselves instead of using FSUtils.getReferenceFilePaths because
           * we know the mob region never splits, so we can only have HFileLink references
           * and looking for just them is cheaper then listing everything.
           *
           * This glob should match the naming convention for HFileLinks to our referenced hfile.
           * As simplified explanation those file names look like "table=region-hfile". For details
           * see the {@link HFileLink#createHFileLinkName HFileLink implementation}.
           */
          FileStatus[] hlinks = mob.getFileSystem(conf).globStatus(new Path(mob + "/*=*-" + file));
          if (hlinks != null && hlinks.length != 0) {
            if (hlinks.length != 1) {
              LOG.warn("Found file '{}' as hfilelinks in the mob area, but there are more than " +
                  "one: {}", file, Arrays.deepToString(hlinks));
            }
            HFileLink found = null;
            for (FileStatus hlink : hlinks) {
              HFileLink tmp = HFileLink.buildFromHFileLinkPattern(conf, hlink.getPath());
              if (tmp.exists(archive.getFileSystem(conf))) {
                found = tmp;
                break;
              } else {
                LOG.debug("Target file does not exist for ref {}", tmp);
              }
            }
            if (found != null) {
              LOG.debug("Found file '{}' as a ref in the mob area: {}", file, found);
              context.write(OK_HLINK_CLONE, key);
            } else {
              LOG.warn("Found file '{}' as ref(s) in the mob area but they do not point to an hfile"
                  + " that exists.", file);
              context.write(DATALOSS_HLINK_DANGLING, encodeRows(context, key, rows));
            }
          } else {
            LOG.error("Could not find referenced file '{}'. See the docs on this tool.", file);
            LOG.debug("Note that we don't have the server-side tag from the mob cells that says "
                + "what table the reference is originally from. So if the HFileLink in this table "
                + "is missing but the referenced file is still in the table from that tag, then "
                + "lookups of these impacted rows will work. Do a scan of the reference details "
                + "of the cell for the hfile name and then check the entire hbase install if this "
                + "table was made from a snapshot of another table. see the ref guide section on "
                + "mob for details.");
            context.write(DATALOSS_MISSING, encodeRows(context, key, rows));
          }
        } catch (IOException e) {
          LOG.error(
              "Exception while checking mob area of our table for HFileLinks that point to {}",
              file, e);
          context.write(DATALOSS_MISSING_IOE, encodeRows(context, key, rows));
        }
      }
    }

    /**
     * reuses the passed Text key. appends the configured seperator and then a comma seperated list
     * of base64 encoded row keys
     */
    private Text encodeRows(Context context, Text key, Iterable<ImmutableBytesWritable> rows)
        throws IOException {
      StringBuilder sb = new StringBuilder(key.toString());
      sb.append(seperator);
      boolean moreThanOne = false;
      long count = 0;
      for (ImmutableBytesWritable row : rows) {
        if (moreThanOne) {
          sb.append(",");
        }
        sb.append(base64.encodeToString(row.copyBytes()));
        moreThanOne = true;
        count++;
      }
      context.getCounter("PROBLEM", "Problem MOB files").increment(1L);
      context.getCounter("PROBLEM", "Affected rows").increment(count);
      context.getCounter("ROWS WITH PROBLEMS PER FILE", "Number of HFiles with " +
          log10GroupedString(count) + "s of affected rows").increment(1L);
      key.set(sb.toString());
      return key;
    }
  }

  /**
   * Returns the string representation of the given number after grouping it
   * into log10 buckets. e.g. 0-9 -> 1, 10-99 -> 10, ..., 100,000-999,999 -> 100,000, etc.
   */
  static String log10GroupedString(long number) {
    return String.format("%,d", (long)(Math.pow(10d, Math.floor(Math.log10(number)))));
  }

  /**
   * Main method for the tool.
   * @return 0 if success, 1 for bad args. 2 if job aborted with an exception,
   *   3 if mr job was unsuccessful
   */
  public int run(String[] args) throws IOException, InterruptedException {
    // TODO make family and table optional
    if (args.length != 3) {
      printUsage();
      return 1;
    }
    final String output = args[0];
    final String tableName = args[1];
    final String familyName = args[2];
    final long reportStartTime = EnvironmentEdgeManager.currentTime();
    Configuration conf = getConf();
    try {
      FileSystem fs = FileSystem.get(conf);
      // check whether the current user is the same one with the owner of hbase root
      String currentUserName = UserGroupInformation.getCurrentUser().getShortUserName();
      FileStatus[] hbaseRootFileStat = fs.listStatus(new Path(conf.get(HConstants.HBASE_DIR)));
      if (hbaseRootFileStat.length > 0) {
        String owner = hbaseRootFileStat[0].getOwner();
        if (!owner.equals(currentUserName)) {
          String errorMsg = "The current user[" + currentUserName
              + "] does not have hbase root credentials."
              + " If this job fails due to an inability to read HBase's internal directories, "
              + "you will need to rerun as a user with sufficient permissions. The HBase superuser "
              + "is a safe choice.";
          LOG.warn(errorMsg);
        }
      } else {
        LOG.error("The passed configs point to an HBase dir does not exist: {}",
            conf.get(HConstants.HBASE_DIR));
        throw new IOException("The target HBase does not exist");
      }

      byte[] family;
      int maxVersions;
      TableName tn = TableName.valueOf(tableName);
      try (Connection connection = ConnectionFactory.createConnection(conf);
           Admin admin = connection.getAdmin()) {
        TableDescriptor htd = admin.getDescriptor(tn);
        ColumnFamilyDescriptor hcd = htd.getColumnFamily(Bytes.toBytes(familyName));
        if (hcd == null || !hcd.isMobEnabled()) {
          throw new IOException("Column family " + familyName + " is not a MOB column family");
        }
        family = hcd.getName();
        maxVersions = hcd.getMaxVersions();
      }


      String id = getClass().getSimpleName() + UUID.randomUUID().toString().replace("-", "");
      Job job = null;
      Scan scan = new Scan();
      scan.addFamily(family);
      // Do not retrieve the mob data when scanning
      scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
      scan.setAttribute(MobConstants.MOB_SCAN_REF_ONLY, Bytes.toBytes(Boolean.TRUE));
      // If a scanner caching value isn't set, pick a smaller default since we know we're doing
      // a full table scan and don't want to impact other clients badly.
      scan.setCaching(conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, 10000));
      scan.setCacheBlocks(false);
      scan.setMaxVersions(maxVersions);
      conf.set(REPORT_JOB_ID, id);

      job = Job.getInstance(conf);
      job.setJarByClass(getClass());
      TableMapReduceUtil.initTableMapperJob(tn, scan,
          MobRefMapper.class, Text.class, ImmutableBytesWritable.class, job);

      job.setReducerClass(MobRefReducer.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job, new Path(output));

      job.setJobName(getClass().getSimpleName() + "-" + tn + "-" + familyName);
      // for use in the reducer. easier than re-parsing it out of the scan string.
      job.getConfiguration().set(TableInputFormat.SCAN_COLUMN_FAMILY, familyName);

      // Use when we start this job as the base point for file "recency".
      job.getConfiguration().setLong(REPORT_START_DATETIME, reportStartTime);

      if (job.waitForCompletion(true)) {
        LOG.info("Finished creating report for '{}', family='{}'", tn, familyName);
      } else {
        System.err.println("Job was not successful");
        return 3;
      }
      return 0;

    } catch (ClassNotFoundException | RuntimeException | IOException | InterruptedException e) {
      System.err.println("Job aborted due to exception " + e);
      return 2; // job failed
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new MobRefReporter(), args);
    System.exit(ret);
  }

  private void printUsage() {
    System.err.println("Usage:\n" + "--------------------------\n" + MobRefReporter.class.getName()
        + " output-dir tableName familyName");
    System.err.println(" output-dir       Where to write output report.");
    System.err.println(" tableName        The table name");
    System.err.println(" familyName       The column family name");
  }

}
