package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.junit.Before;
import org.junit.Rule;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

public class TestTracingBase {

  protected static final ServerName MASTER_HOST = ServerName.valueOf("localhost", 16010, 12345);
  protected static final RegionLocations META_REGION_LOCATION =
    new RegionLocations(new HRegionLocation(RegionInfoBuilder.FIRST_META_REGIONINFO, MASTER_HOST));

  protected Configuration conf;

  @Rule
  public OpenTelemetryRule traceRule = OpenTelemetryRule.create();

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      RegistryForTracingTest.class.getName());
  }

  protected void assertTrace(String className, String methodName, ServerName serverName,
    TableName tableName) {
    String expectedSpanName = String.format("%s.%s", className, methodName);
    Waiter.waitFor(conf, 1000,
      () -> traceRule.getSpans().stream()
        .anyMatch(span -> span.getName().equals(expectedSpanName) &&
          span.getKind() == SpanKind.INTERNAL && span.hasEnded()));
    SpanData data = traceRule.getSpans().stream()
      .filter(s -> s.getName().equals(expectedSpanName)).findFirst().get();
    assertEquals(StatusCode.OK, data.getStatus().getStatusCode());

    if (serverName != null) {
      assertEquals(serverName.getServerName(), data.getAttributes().get(TraceUtil.SERVER_NAME_KEY));
    }

    if (tableName != null) {
      assertEquals(tableName.getNamespaceAsString(),
        data.getAttributes().get(TraceUtil.NAMESPACE_KEY));
      assertEquals(tableName.getNameAsString(), data.getAttributes().get(TraceUtil.TABLE_KEY));
    }
  }

  static class RegistryForTracingTest implements ConnectionRegistry {

    public RegistryForTracingTest(Configuration conf) {
    }

    @Override
    public CompletableFuture<RegionLocations> getMetaRegionLocations() {
      return CompletableFuture.completedFuture(META_REGION_LOCATION);
    }

    @Override
    public CompletableFuture<String> getClusterId() {
      return CompletableFuture.completedFuture(HConstants.CLUSTER_ID_DEFAULT);
    }

    @Override
    public CompletableFuture<ServerName> getActiveMaster() {
      return CompletableFuture.completedFuture(MASTER_HOST);
    }

    @Override public void close() {

    }
  }

}
