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

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.mapreduce.ExportUtils;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.DelegationToken;
import org.apache.hadoop.hbase.protobuf.generated.ExportProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Export an HBase table. Writes content to sequence files up in HDFS. Use
 * {@link Import} to read it back in again. It is implemented by the endpoint
 * technique.
 *
 * @see org.apache.hadoop.hbase.mapreduce.Export
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class Export extends ExportProtos.ExportService implements RegionCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(Export.class);
  private static final Class<? extends CompressionCodec> DEFAULT_CODEC = DefaultCodec.class;
  private static final SequenceFile.CompressionType DEFAULT_TYPE =
      SequenceFile.CompressionType.RECORD;
  private RegionCoprocessorEnvironment env = null;
  private UserProvider userProvider;

  public static void main(String[] args) throws Throwable {
    Map<byte[], Response> response = run(HBaseConfiguration.create(), args);
    System.exit(response == null ? -1 : 0);
  }

  @InterfaceAudience.Private
  static Map<byte[], Response> run(final Configuration conf, final String[] args) throws Throwable {
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (!ExportUtils.isValidArguements(args)) {
      ExportUtils.usage("Wrong number of arguments: " + ArrayUtils.getLength(otherArgs));
      return null;
    }
    Triple<TableName, Scan, Path> arguments =
        ExportUtils.getArgumentsFromCommandLine(conf, otherArgs);
    return run(conf, arguments.getFirst(), arguments.getSecond(), arguments.getThird());
  }

  public static Map<byte[], Response> run(final Configuration conf, TableName tableName,
      Scan scan, Path dir) throws Throwable {
    FileSystem fs = dir.getFileSystem(conf);
    UserProvider userProvider = UserProvider.instantiate(conf);
    checkDir(fs, dir);
    FsDelegationToken fsDelegationToken = new FsDelegationToken(userProvider, "renewer");
    fsDelegationToken.acquireDelegationToken(fs);
    try {
      final ExportProtos.ExportRequest request = getConfiguredRequest(conf, dir,
        scan, fsDelegationToken.getUserToken());
      try (Connection con = ConnectionFactory.createConnection(conf);
              Table table = con.getTable(tableName)) {
        Map<byte[], Response> result = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        table.coprocessorService(ExportProtos.ExportService.class,
          scan.getStartRow(),
          scan.getStopRow(),
          (ExportProtos.ExportService service) -> {
            ServerRpcController controller = new ServerRpcController();
            Map<byte[], ExportProtos.ExportResponse> rval = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            CoprocessorRpcUtils.BlockingRpcCallback<ExportProtos.ExportResponse>
              rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
            service.export(controller, request, rpcCallback);
            if (controller.failedOnException()) {
              throw controller.getFailedOn();
            }
            return rpcCallback.get();
          }).forEach((k, v) -> result.put(k, new Response(v)));
        return result;
      } catch (Throwable e) {
        fs.delete(dir, true);
        throw e;
      }
    } finally {
      fsDelegationToken.releaseDelegationToken();
    }
  }

  private static boolean getCompression(final ExportProtos.ExportRequest request) {
    if (request.hasCompressed()) {
      return request.getCompressed();
    } else {
      return false;
    }
  }

  private static SequenceFile.CompressionType getCompressionType(
      final ExportProtos.ExportRequest request) {
    if (request.hasCompressType()) {
      return SequenceFile.CompressionType.valueOf(request.getCompressType());
    } else {
      return DEFAULT_TYPE;
    }
  }

  private static CompressionCodec getCompressionCodec(final Configuration conf,
      final ExportProtos.ExportRequest request) {
    try {
      Class<? extends CompressionCodec> codecClass;
      if (request.hasCompressCodec()) {
        codecClass = conf.getClassByName(request.getCompressCodec())
            .asSubclass(CompressionCodec.class);
      } else {
        codecClass = DEFAULT_CODEC;
      }
      return ReflectionUtils.newInstance(codecClass, conf);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Compression codec "
              + request.getCompressCodec() + " was not found.", e);
    }
  }

  private static SequenceFile.Writer.Option getOutputPath(final Configuration conf,
          final RegionInfo info, final ExportProtos.ExportRequest request) throws IOException {
    Path file = new Path(request.getOutputPath(), "export-" + info.getEncodedName());
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      throw new IOException(file + " exists");
    }
    return SequenceFile.Writer.file(file);
  }

  private static List<SequenceFile.Writer.Option> getWriterOptions(final Configuration conf,
          final RegionInfo info, final ExportProtos.ExportRequest request) throws IOException {
    List<SequenceFile.Writer.Option> rval = new LinkedList<>();
    rval.add(SequenceFile.Writer.keyClass(ImmutableBytesWritable.class));
    rval.add(SequenceFile.Writer.valueClass(Result.class));
    rval.add(getOutputPath(conf, info, request));
    if (getCompression(request)) {
      rval.add(SequenceFile.Writer.compression(getCompressionType(request),
          getCompressionCodec(conf, request)));
    } else {
      rval.add(SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
    }
    return rval;
  }

  private static ExportProtos.ExportResponse processData(final Region region,
      final Configuration conf, final UserProvider userProvider, final Scan scan,
      final Token userToken, final List<SequenceFile.Writer.Option> opts) throws IOException {
    ScanCoprocessor cp = new ScanCoprocessor(region);
    RegionScanner scanner = null;
    try (RegionOp regionOp = new RegionOp(region);
            SecureWriter out = new SecureWriter(conf, userProvider, userToken, opts)) {
      scanner = cp.checkScannerOpen(scan);
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      long rowCount = 0;
      long cellCount = 0;
      List<Result> results = new ArrayList<>();
      List<Cell> cells = new ArrayList<>();
      boolean hasMore;
      do {
        boolean bypass = cp.preScannerNext(scanner, results, scan.getBatch());
        if (bypass) {
          hasMore = false;
        } else {
          hasMore = scanner.nextRaw(cells);
          if (cells.isEmpty()) {
            continue;
          }
          Cell firstCell = cells.get(0);
          for (Cell cell : cells) {
            if (Bytes.compareTo(firstCell.getRowArray(), firstCell.getRowOffset(),
                firstCell.getRowLength(), cell.getRowArray(), cell.getRowOffset(),
                cell.getRowLength()) != 0) {
              throw new IOException("Why the RegionScanner#nextRaw returns the data of different"
                  + " rows?? first row="
                  + Bytes.toHex(firstCell.getRowArray(), firstCell.getRowOffset(),
                    firstCell.getRowLength())
                  + ", current row="
                  + Bytes.toHex(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            }
          }
          results.add(Result.create(cells));
          cells.clear();
          cp.postScannerNext(scanner, results, scan.getBatch(), hasMore);
        }
        for (Result r : results) {
          key.set(r.getRow());
          out.append(key, r);
          ++rowCount;
          cellCount += r.size();
        }
        results.clear();
      } while (hasMore);
      return ExportProtos.ExportResponse.newBuilder()
              .setRowCount(rowCount)
              .setCellCount(cellCount)
              .build();
    } finally {
      cp.checkScannerClose(scanner);
    }
  }

  private static void checkDir(final FileSystem fs, final Path dir) throws IOException {
    if (fs.exists(dir)) {
      throw new RuntimeException("The " + dir + " exists");
    }
    if (!fs.mkdirs(dir)) {
      throw new IOException("Failed to create the " + dir);
    }
  }

  private static ExportProtos.ExportRequest getConfiguredRequest(Configuration conf,
          Path dir, final Scan scan, final Token<?> userToken) throws IOException {
    boolean compressed = conf.getBoolean(FileOutputFormat.COMPRESS, false);
    String compressionType = conf.get(FileOutputFormat.COMPRESS_TYPE,
            DEFAULT_TYPE.toString());
    String compressionCodec = conf.get(FileOutputFormat.COMPRESS_CODEC,
            DEFAULT_CODEC.getName());
    DelegationToken protoToken = null;
    if (userToken != null) {
      protoToken = DelegationToken.newBuilder()
              .setIdentifier(ByteStringer.wrap(userToken.getIdentifier()))
              .setPassword(ByteStringer.wrap(userToken.getPassword()))
              .setKind(userToken.getKind().toString())
              .setService(userToken.getService().toString()).build();
    }
    LOG.info("compressed=" + compressed
            + ", compression type=" + compressionType
            + ", compression codec=" + compressionCodec
            + ", userToken=" + userToken);
    ExportProtos.ExportRequest.Builder builder = ExportProtos.ExportRequest.newBuilder()
            .setScan(ProtobufUtil.toScan(scan))
            .setOutputPath(dir.toString())
            .setCompressed(compressed)
            .setCompressCodec(compressionCodec)
            .setCompressType(compressionType);
    if (protoToken != null) {
      builder.setFsToken(protoToken);
    }
    return builder.build();
  }

  @Override
  public void start(CoprocessorEnvironment environment) throws IOException {
    if (environment instanceof RegionCoprocessorEnvironment) {
      env = (RegionCoprocessorEnvironment) environment;
      userProvider = UserProvider.instantiate(env.getConfiguration());
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  @Override
  public void export(RpcController controller, ExportProtos.ExportRequest request,
          RpcCallback<ExportProtos.ExportResponse> done) {
    Region region = env.getRegion();
    Configuration conf = HBaseConfiguration.create(env.getConfiguration());
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        ResultSerialization.class.getName());
    try {
      Scan scan = validateKey(region.getRegionInfo(), request);
      Token userToken = null;
      if (userProvider.isHadoopSecurityEnabled() && !request.hasFsToken()) {
        LOG.warn("Hadoop security is enable, but no found of user token");
      } else if (userProvider.isHadoopSecurityEnabled()) {
        userToken = new Token(request.getFsToken().getIdentifier().toByteArray(),
                request.getFsToken().getPassword().toByteArray(),
                new Text(request.getFsToken().getKind()),
                new Text(request.getFsToken().getService()));
      }
      ExportProtos.ExportResponse response = processData(region, conf, userProvider,
        scan, userToken, getWriterOptions(conf, region.getRegionInfo(), request));
      done.run(response);
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
      LOG.error(e.toString(), e);
    }
  }

  private Scan validateKey(final RegionInfo region, final ExportProtos.ExportRequest request)
      throws IOException {
    Scan scan = ProtobufUtil.toScan(request.getScan());
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

  private static class RegionOp implements Closeable {
    private final Region region;

    RegionOp(final Region region) throws IOException {
      this.region = region;
      region.startRegionOperation();
    }

    @Override
    public void close() throws IOException {
      region.closeRegionOperation();
    }
  }

  private static class ScanCoprocessor {
    private final HRegion region;

    ScanCoprocessor(final Region region) {
      this.region = (HRegion) region;
    }

    RegionScanner checkScannerOpen(final Scan scan) throws IOException {
      RegionScanner scanner;
      if (region.getCoprocessorHost() == null) {
        scanner = region.getScanner(scan);
      } else {
        region.getCoprocessorHost().preScannerOpen(scan);
        scanner = region.getScanner(scan);
        scanner = region.getCoprocessorHost().postScannerOpen(scan, scanner);
      }
      if (scanner == null) {
        throw new IOException("Failed to open region scanner");
      }
      return scanner;
    }

    void checkScannerClose(final InternalScanner s) throws IOException {
      if (s == null) {
        return;
      }
      if (region.getCoprocessorHost() == null) {
        s.close();
        return;
      }
      region.getCoprocessorHost().preScannerClose(s);
      try {
        s.close();
      } finally {
        region.getCoprocessorHost().postScannerClose(s);
      }
    }

    boolean preScannerNext(final InternalScanner s,
            final List<Result> results, final int limit) throws IOException {
      if (region.getCoprocessorHost() == null) {
        return false;
      } else {
        Boolean bypass = region.getCoprocessorHost().preScannerNext(s, results, limit);
        return bypass == null ? false : bypass;
      }
    }

    boolean postScannerNext(final InternalScanner s,
            final List<Result> results, final int limit, boolean hasMore)
            throws IOException {
      if (region.getCoprocessorHost() == null) {
        return false;
      } else {
        return region.getCoprocessorHost().postScannerNext(s, results, limit, hasMore);
      }
    }
  }

  private static class SecureWriter implements Closeable {
    private final PrivilegedWriter privilegedWriter;

    SecureWriter(final Configuration conf, final UserProvider userProvider,
        final Token userToken, final List<SequenceFile.Writer.Option> opts)
        throws IOException {
      User user = getActiveUser(userProvider, userToken);
      try {
        SequenceFile.Writer sequenceFileWriter =
            user.runAs((PrivilegedExceptionAction<SequenceFile.Writer>) () ->
                SequenceFile.createWriter(conf,
                    opts.toArray(new SequenceFile.Writer.Option[opts.size()])));
        privilegedWriter = new PrivilegedWriter(user, sequenceFileWriter);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    void append(final Object key, final Object value) throws IOException {
      privilegedWriter.append(key, value);
    }

    private static User getActiveUser(final UserProvider userProvider, final Token userToken)
        throws IOException {
      User user = RpcServer.getRequestUser().orElse(userProvider.getCurrent());
      if (user == null && userToken != null) {
        LOG.warn("No found of user credentials, but a token was got from user request");
      } else if (user != null && userToken != null) {
        user.addToken(userToken);
      }
      return user;
    }

    @Override
    public void close() throws IOException {
      privilegedWriter.close();
    }
  }

  private static class PrivilegedWriter implements PrivilegedExceptionAction<Boolean>,
          Closeable {
    private final User user;
    private final SequenceFile.Writer out;
    private Object key;
    private Object value;

    PrivilegedWriter(final User user, final SequenceFile.Writer out) {
      this.user = user;
      this.out = out;
    }

    void append(final Object key, final Object value) throws IOException {
      if (user == null) {
        out.append(key, value);
      } else {
        this.key = key;
        this.value = value;
        try {
          user.runAs(this);
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }
    }

    @Override
    public Boolean run() throws Exception {
      out.append(key, value);
      return true;
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
  }

  public final static class Response {
    private final long rowCount;
    private final long cellCount;

    private Response(ExportProtos.ExportResponse r) {
      this.rowCount = r.getRowCount();
      this.cellCount = r.getCellCount();
    }

    public long getRowCount() {
      return rowCount;
    }

    public long getCellCount() {
      return cellCount;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder(35);
      return builder.append("rowCount=")
             .append(rowCount)
             .append(", cellCount=")
             .append(cellCount)
             .toString();
    }
  }
}
