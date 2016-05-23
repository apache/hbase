/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ExportProtos;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.mapreduce.Export;
import org.apache.hadoop.hbase.mapreduce.Import;

/**
* Export an HBase table.
* Writes content to sequence files up in HDFS.  Use {@link Import} to read it
* back in again.
* It is implemented by the endpoint technique.
* @see Export
*/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ExportEndpoint extends ExportProtos.ExportService
  implements Coprocessor, CoprocessorService {
  private static final Log LOG = LogFactory.getLog(ExportEndpoint.class);
  private RegionCoprocessorEnvironment env = null;
  @Override
  public void start(CoprocessorEnvironment environment) throws IOException {
    if (environment instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) environment;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }
  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public Service getService() {
    return this;
  }
  private static boolean getCompression(final ExportProtos.ExportRequest request) {
    if (request.hasCompressed()) {
      return request.getCompressed();
    } else {
      return false;
    }
  }
  private static SequenceFile.CompressionType getCompressionType(final ExportProtos.ExportRequest request) {
    if (!request.hasCompressType()) {
      return null;
    }
    return SequenceFile.CompressionType.valueOf(request.getCompressType());
  }
  private static CompressionCodec getCompressionCodec(final Configuration conf, final ExportProtos.ExportRequest request) {
    if (!request.hasCompressCodec()) {
      return null;
    }
    try {
      Class<? extends CompressionCodec> codecClass = conf.getClassByName(request.getCompressCodec()).asSubclass(CompressionCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Compression codec "
              + request.getCompressCodec()+ " was not found.", e);
    }
  }
  private static SequenceFile.Writer.Option getOutputPath(final Configuration conf,
          final HRegionInfo info, final ExportProtos.ExportRequest request) throws IOException {
    Path file = new Path(request.getOutputPath(), "export-" + info.getEncodedName());
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(file)) {
      throw new IOException(file + " exists");
    }
    return SequenceFile.Writer.file(file);
  }
  private static List<SequenceFile.Writer.Option> getWriterOptions(final Configuration conf,
          final HRegionInfo info, final ExportProtos.ExportRequest request) throws IOException {
    List<SequenceFile.Writer.Option> rval = new LinkedList<>();
    rval.add(SequenceFile.Writer.keyClass(ImmutableBytesWritable.class));
    rval.add(SequenceFile.Writer.valueClass(Result.class));
    rval.add(getOutputPath(conf, info, request));
    boolean compressed = getCompression(request);
    if (compressed) {
      SequenceFile.CompressionType type = getCompressionType(request);
      if (type != null) {
        CompressionCodec codec = getCompressionCodec(conf, request);
        rval.add(SequenceFile.Writer.compression(type, codec));
      }
    }
    return rval;
  }
  private Scan validateKey(final HRegionInfo region, final Scan scan) {
    byte[] regionStartKey = region.getStartKey();
    byte[] originStartKey = scan.getStartRow();
    if (originStartKey == null
      || Bytes.compareTo(originStartKey, regionStartKey) < 0) {
      scan.setStartRow(regionStartKey);
    }
    byte[] regionEndKey = region.getEndKey();
    byte[] originEndKey = scan.getStopRow();
    if (originEndKey == null
      || Bytes.compareTo(originEndKey, regionEndKey) > 0) {
      scan.setStartRow(regionEndKey);
    }
    return scan;
  }
  @Override
  public void export(RpcController controller, ExportProtos.ExportRequest request,
    RpcCallback<ExportProtos.ExportResponse> done) {
    Region region = env.getRegion();
    Configuration conf = HBaseConfiguration.create(env.getConfiguration());
    conf.setStrings("io.serializations", conf.get("io.serializations"), ResultSerialization.class.getName());
    try {
      Scan scan = validateKey(region.getRegionInfo(), ProtobufUtil.toScan(request.getScan()));
      ExportProtos.ExportResponse response = processData(conf, region, scan,
              getWriterOptions(conf, region.getRegionInfo(), request));
      done.run(response);
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
      LOG.error(e);
    }
  }
  private static ExportProtos.ExportResponse processData(final Configuration conf,
      final Region region, final Scan scan, final List<SequenceFile.Writer.Option> opts) throws IOException {
    region.startRegionOperation();
    try (SequenceFile.Writer out = SequenceFile.createWriter(conf,
            opts.toArray(new SequenceFile.Writer.Option[opts.size()]));
      RegionScanner scanner = region.getScanner(scan)) {
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      long rowCount = 0;
      long cellCount = 0;
      List<Cell> buf = new ArrayList<>();
      boolean hasMore;
      do {
        hasMore = scanner.nextRaw(buf);
        if (!buf.isEmpty()) {
          Cell firstCell = buf.get(0);
          for (Cell cell : buf) {
            if (Bytes.compareTo(
              firstCell.getRowArray(), firstCell.getRowOffset(), firstCell.getRowLength(),
              cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) != 0) {
              throw new IOException("Why the RegionScanner#nextRaw returns the data of different rows??");
            }
          }
          key.set(firstCell.getRowArray(), firstCell.getRowOffset(), firstCell.getRowLength());
          out.append(key, Result.create(buf));
          ++rowCount;
          cellCount += buf.size();
          buf.clear();
        }
      } while (hasMore);
      return ExportProtos.ExportResponse.newBuilder()
        .setRowCount(rowCount)
        .setCellCount(cellCount)
        .build();
    } finally {
      region.closeRegionOperation();
    }
  }
  public static void main(String[] args) throws IOException, Throwable {
    run(HBaseConfiguration.create(), args);
  }
  public static Map<byte[], ExportProtos.ExportResponse> run(final Configuration conf,
          final String[] args) throws IOException, Throwable {
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (!Export.checkArguments(otherArgs)) {
      Export.usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    TableName tableName = TableName.valueOf(otherArgs[0]);
    FileSystem fs = FileSystem.get(conf);
    String dir = otherArgs[1];
    checkDir(fs, dir);
    Scan scan = Export.getConfiguredScanForJob(conf, otherArgs);
    final ExportProtos.ExportRequest request = getConfiguredRequestForJob(conf, otherArgs, scan);
    try (Connection con = ConnectionFactory.createConnection(conf);
            Table table = con.getTable(tableName)) {
      return table.coprocessorService(ExportProtos.ExportService.class,
            scan.getStartRow(),
            scan.getStopRow(), new Batch.Call<ExportProtos.ExportService, ExportProtos.ExportResponse>() {
            @Override
            public ExportProtos.ExportResponse call(ExportProtos.ExportService service) throws IOException {
              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<ExportProtos.ExportResponse> rpcCallback = new BlockingRpcCallback<>();
              service.export(controller, request, rpcCallback);
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return rpcCallback.get();
            }
        });
    } catch (Throwable e) {
      fs.delete(new Path(dir), true);
      throw e;
    }
  }
  private static void checkDir(final FileSystem fs, final String path) throws IOException {
    Path dir = fs.makeQualified(new Path(path));
    if (fs.exists(dir)) {
      throw new RuntimeException("The " + path + " exists");
    }
    fs.mkdirs(dir);
    fs.setPermission(dir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
  }
  private static ExportProtos.ExportRequest getConfiguredRequestForJob(Configuration conf,
          String[] args, final Scan scan) throws IOException {
    String dir = args[1];
    boolean compressed = conf.getBoolean(FileOutputFormat.COMPRESS, true);
    String compressionType = conf.get(FileOutputFormat.COMPRESS_TYPE,
        SequenceFile.CompressionType.RECORD.toString());
    String compressionCodec = conf.get(FileOutputFormat.COMPRESS_CODEC,
            DefaultCodec.class.getName());
    LOG.info("compressed=" + compressed
        + ", compression type=" + compressionType
        + ", compression codec=" + compressionCodec);
    return ExportProtos.ExportRequest.newBuilder()
            .setScan(ProtobufUtil.toScan(scan))
            .setOutputPath(dir)
            .setCompressed(compressed)
            .setCompressCodec(compressionCodec)
            .setCompressType(compressionType)
            .build();
  }
}
