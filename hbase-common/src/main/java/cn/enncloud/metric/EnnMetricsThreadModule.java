package cn.enncloud.metric;

import cn.enncloud.metric.config.EnnMetricsConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.avaje.metric.report.MetricReporter;

/** Created by jessejia on 16/9/2. */
public class EnnMetricsThreadModule implements Module {
  private EnnMetricsConfig config;

  public EnnMetricsThreadModule(EnnMetricsConfig config) {
    this.config = config;
  }

  @Override
  public void configure(Binder binder) {
    binder.bind(MetricReporter.class).to(OpentsdbHttpReporter.class);
  }

  @Provides
  EnnMetricsConfig getEnnMetricsConfig() {
    return this.config;
  }
}
