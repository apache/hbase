package cn.enncloud.metric;

import cn.enncloud.metric.config.OpentsdbConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/** Created by jessejia on 16/8/30. */
public class OpentsdbHttpReporterModule implements Module {
  private OpentsdbConfig opentsdbConfig;

  public OpentsdbHttpReporterModule(OpentsdbConfig opentsdbConfig) {
    this.opentsdbConfig = opentsdbConfig;
  }

  @Override
  public void configure(Binder binder) {}

  @Provides
  HttpClient provideHttpClient() {
    // Config http client used by opentsdb reporter such as default headers
    return HttpClientBuilder.create()
        .setConnectionManager(new PoolingHttpClientConnectionManager())
        .build();
  }

  @Provides
  OpentsdbConfig provideOpentsdbConfig() {
    return this.opentsdbConfig;
  }
}
