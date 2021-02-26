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
package org.apache.hadoop.hbase.procedure2.store.region;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.OptionGroup;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * A tool to dump the procedures in the HFiles.
 * <p/>
 * The different between this and {@link org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter} is
 * that, this class will decode the procedure in the cell for better debugging. You are free to use
 * {@link org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter} to dump the same file as well.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class HFileProcedurePrettyPrinter extends AbstractHBaseTool {

  private Long procId;

  private List<Path> files = new ArrayList<>();

  private final PrintStream out;

  public HFileProcedurePrettyPrinter() {
    this(System.out);
  }

  public HFileProcedurePrettyPrinter(PrintStream out) {
    this.out = out;
  }

  @Override
  protected void addOptions() {
    addOptWithArg("w", "seekToPid", "Seek to this procedure id and print this procedure only");
    OptionGroup files = new OptionGroup();
    files.addOption(new Option("f", "file", true,
      "File to scan. Pass full-path; e.g. hdfs://a:9000/MasterProcs/master/local/proc/xxx"));
    files.addOption(new Option("a", "all", false, "Scan the whole procedure region."));
    files.setRequired(true);
    options.addOptionGroup(files);
  }

  private void addAllHFiles() throws IOException {
    Path masterProcDir = new Path(CommonFSUtils.getRootDir(conf), MasterRegionFactory.MASTER_STORE_DIR);
    Path tableDir = CommonFSUtils.getTableDir(masterProcDir, MasterRegionFactory.TABLE_NAME);
    FileSystem fs = tableDir.getFileSystem(conf);
    Path regionDir =
      fs.listStatus(tableDir, p -> RegionInfo.isEncodedRegionName(Bytes.toBytes(p.getName())))[0]
        .getPath();
    List<Path> regionFiles = HFile.getStoreFiles(fs, regionDir);
    files.addAll(regionFiles);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    if (cmd.hasOption("w")) {
      String key = cmd.getOptionValue("w");
      if (key != null && key.length() != 0) {
        procId = Long.parseLong(key);
      } else {
        throw new IllegalArgumentException("Invalid row is specified.");
      }
    }
    if (cmd.hasOption("f")) {
      files.add(new Path(cmd.getOptionValue("f")));
    }
    if (cmd.hasOption("a")) {
      try {
        addAllHFiles();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private void printCell(Cell cell) throws IOException {
    out.print("K: " + CellUtil.getCellKeyAsString(cell,
      c -> Long.toString(Bytes.toLong(c.getRowArray(), c.getRowOffset(), c.getRowLength()))));
    if (cell.getType() == Cell.Type.Put) {
      if (cell.getValueLength() == 0) {
        out.println(" V: mark deleted");
      } else {
        Procedure<?> proc = ProcedureUtil.convertToProcedure(ProcedureProtos.Procedure.parser()
          .parseFrom(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        out.println(" V: " + proc.toStringDetails());
      }
    } else {
      out.println();
    }
  }

  private void processFile(Path file) throws IOException {
    out.println("Scanning -> " + file);
    FileSystem fs = file.getFileSystem(conf);
    try (HFile.Reader reader = HFile.createReader(fs, file, CacheConfig.DISABLED, true, conf);
      HFileScanner scanner = reader.getScanner(false, false, false)) {
      if (procId != null) {
        if (scanner
          .seekTo(PrivateCellUtil.createFirstOnRow(Bytes.toBytes(procId.longValue()))) != -1) {
          do {
            Cell cell = scanner.getCell();
            long currentProcId =
              Bytes.toLong(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            if (currentProcId != procId.longValue()) {
              break;
            }
            printCell(cell);
          } while (scanner.next());
        }
      } else {
        if (scanner.seekTo()) {
          do {
            Cell cell = scanner.getCell();
            printCell(cell);
          } while (scanner.next());
        }
      }
    }
  }

  @Override
  protected int doWork() throws Exception {
    for (Path file : files) {
      processFile(file);
    }
    return 0;
  }

  public static void main(String[] args) {
    new HFileProcedurePrettyPrinter().doStaticMain(args);
  }
}
