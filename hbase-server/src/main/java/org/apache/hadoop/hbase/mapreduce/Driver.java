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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Driver for hbase mapreduce jobs. Select which to run by passing
 * name of job to this main.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Driver {
  /**
   * @param args
   * @throws Throwable
   */
  public static void main(String[] args) throws Throwable {
    ProgramDriver pgd = new ProgramDriver();
    pgd.addClass(RowCounter.NAME, RowCounter.class,
      "Count rows in HBase table");
    pgd.addClass(CellCounter.NAME, CellCounter.class,
      "Count cells in HBase table");
    pgd.addClass(Export.NAME, Export.class, "Write table data to HDFS.");
    pgd.addClass(Import.NAME, Import.class, "Import data written by Export.");
    pgd.addClass(ImportTsv.NAME, ImportTsv.class, "Import data in TSV format.");
    pgd.addClass(LoadIncrementalHFiles.NAME, LoadIncrementalHFiles.class,
                 "Complete a bulk data load.");
    pgd.addClass(CopyTable.NAME, CopyTable.class,
        "Export a table from local cluster to peer cluster");
    pgd.addClass(VerifyReplication.NAME, VerifyReplication.class, "Compare" +
        " the data from tables in two different clusters. WARNING: It" +
        " doesn't work for incrementColumnValues'd cells since the" +
        " timestamp is changed after being appended to the log.");
    ProgramDriver.class.getMethod("driver", new Class [] {String[].class}).
      invoke(pgd, new Object[]{args});
  }
}
