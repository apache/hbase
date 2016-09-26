package cn.enncloud.metric;

import cn.enncloud.metric.config.EnnMetricsConfig;
import cn.enncloud.metric.config.OpentsdbConfig;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by jessejia on 16/9/26.
 */
public class DefaultEnnHBaseMetrics {
  private static final String ENN_METRICS_OPENTSDB_HOST = "enn.metrics.opentsdb.host";
  private static final String ENN_METRICS_OPENTSDB_PORT = "enn.metrics.opentsdb.port";
  private static final String ENN_METRICS_METRICS_FREQ = "enn.metrics.freq";

  public static void startMetricsCollector(Configuration conf)
      throws IOException {
    String opentsdbHost = conf.get(ENN_METRICS_OPENTSDB_HOST);
    int opentsdbPort = conf.getInt(ENN_METRICS_OPENTSDB_PORT, -1);
    if (StringUtils.isEmpty(opentsdbHost) || opentsdbPort == -1) {
      return;
    }
    int metricsFreq = conf.getInt(ENN_METRICS_METRICS_FREQ, 1);

    Injector injector =
        Guice.createInjector(
            new EnnMetricsThreadModule(EnnMetricsConfig.getConfigWithFreq(metricsFreq)),
            new OpentsdbHttpReporterModule(
                OpentsdbConfig.newBuilder()
                    .setHostname(opentsdbHost)
                    .setPort(opentsdbPort)
                    .build()));

    EnnMetricsThread metricsTread = injector.getInstance(EnnMetricsThread.class);
    new Thread(metricsTread).start();
  }
}
