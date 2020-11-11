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

import static org.apache.hadoop.hbase.master.region.MasterRegionFactory.PROC_FAMILY;

import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * A tool to dump the procedures in the WAL files.
 * <p/>
 * The different between this and {@link WALPrettyPrinter} is that, this class will decode the
 * procedure in the WALEdit for better debugging. You are free to use {@link WALPrettyPrinter} to
 * dump the same file as well.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class WALProcedurePrettyPrinter extends AbstractHBaseTool {

  private static final String KEY_TMPL = "Sequence=%s, at write timestamp=%s";

  private static final DateTimeFormatter FORMATTER =
    DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault());

  private String file;

  private PrintStream out;

  public WALProcedurePrettyPrinter() {
    this(System.out);
  }

  public WALProcedurePrettyPrinter(PrintStream out) {
    this.out = out;
  }

  @Override
  protected void addOptions() {
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    if (cmd.getArgList().size() != 1) {
      throw new IllegalArgumentException("Please specify the file to dump");
    }
    file = cmd.getArgList().get(0);
  }

  @Override
  protected int doWork() throws Exception {
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(conf);
    try (WAL.Reader reader = WALFactory.createReader(fs, path, conf)) {
      for (;;) {
        WAL.Entry entry = reader.next();
        if (entry == null) {
          return 0;
        }
        WALKey key = entry.getKey();
        WALEdit edit = entry.getEdit();
        long sequenceId = key.getSequenceId();
        long writeTime = key.getWriteTime();
        out.println(
          String.format(KEY_TMPL, sequenceId, FORMATTER.format(Instant.ofEpochMilli(writeTime))));
        for (Cell cell : edit.getCells()) {
          Map<String, Object> op = WALPrettyPrinter.toStringMap(cell);
          if (!Bytes.equals(PROC_FAMILY, 0, PROC_FAMILY.length, cell.getFamilyArray(),
            cell.getFamilyOffset(), cell.getFamilyLength())) {
            // We could have cells other than procedure edits, for example, a flush marker
            WALPrettyPrinter.printCell(out, op, false, false);
            continue;
          }
          long procId = Bytes.toLong(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          out.println("pid=" + procId + ", type=" + op.get("type") + ", column=" +
            op.get("family") + ":" + op.get("qualifier"));
          if (cell.getType() == Cell.Type.Put) {
            if (cell.getValueLength() > 0) {
              // should be a normal put
              Procedure<?> proc =
                ProcedureUtil.convertToProcedure(ProcedureProtos.Procedure.parser()
                  .parseFrom(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
              out.println("\t" + proc.toStringDetails());
            } else {
              // should be a 'delete' put
              out.println("\tmark deleted");
            }
          }
          out.println("cell total size sum: " + cell.heapSize());
        }
        out.println("edit heap size: " + edit.heapSize());
        out.println("position: " + reader.getPosition());
      }
    }
  }

  public static void main(String[] args) {
    new WALProcedurePrettyPrinter().doStaticMain(args);
  }
}
