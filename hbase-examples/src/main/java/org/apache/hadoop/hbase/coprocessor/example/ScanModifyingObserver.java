/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A RegionObserver which modifies incoming Scan requests to include additional
 * columns than what the user actually requested.
 */
@InterfaceAudience.Private
public class ScanModifyingObserver implements RegionCoprocessor, RegionObserver {

  public static final String FAMILY_TO_ADD_KEY = "hbase.examples.coprocessor.scanmodifying.family";
  public static final String QUALIFIER_TO_ADD_KEY =
      "hbase.examples.coprocessor.scanmodifying.qualifier";

  private byte[] FAMILY_TO_ADD = null;
  private byte[] QUALIFIER_TO_ADD = null;

  @Override
  public void start(
      @SuppressWarnings("rawtypes") CoprocessorEnvironment env) throws IOException {
    RegionCoprocessorEnvironment renv = (RegionCoprocessorEnvironment) env;
    FAMILY_TO_ADD = Bytes.toBytes(renv.getConfiguration().get(FAMILY_TO_ADD_KEY));
    QUALIFIER_TO_ADD = Bytes.toBytes(renv.getConfiguration().get(QUALIFIER_TO_ADD_KEY));
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    // Extremely important to be sure that the coprocessor is invoked as a RegionObserver
    return Optional.of(this);
  }

  @Override
  public void preScannerOpen(
      ObserverContext<RegionCoprocessorEnvironment> c, Scan scan) throws IOException {
    // Add another family:qualifier
    scan.addColumn(FAMILY_TO_ADD, QUALIFIER_TO_ADD);
  }
}
