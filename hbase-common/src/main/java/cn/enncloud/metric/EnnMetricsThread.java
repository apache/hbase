package cn.enncloud.metric;

import cn.enncloud.metric.config.EnnMetricsConfig;
import com.google.inject.Inject;
import org.avaje.metric.report.HeaderInfo;
import org.avaje.metric.report.MetricReportConfig;
import org.avaje.metric.report.MetricReportManager;
import org.avaje.metric.report.MetricReporter;

/** Created by jessejia on 16/9/2. */
public class EnnMetricsThread implements Runnable {
  private EnnMetricsConfig config;
  private MetricReporter reporter;

  @Inject
  private EnnMetricsThread(EnnMetricsConfig config, MetricReporter reporter) {
    this.config = config;
    this.reporter = reporter;
  }

  @Override
  public void run() {
    MetricReportConfig metricReportConfig = new MetricReportConfig();
    metricReportConfig.setFreqInSeconds(config.getFreqInSeconds());
    HeaderInfo headerInfo = new HeaderInfo();
    headerInfo.setServer(config.getHostname());
    headerInfo.setKey(config.getIpAddress());
    metricReportConfig.setHeaderInfo(headerInfo);
    metricReportConfig.setLocalReporter(reporter);
    new MetricReportManager(metricReportConfig);
  }
}
