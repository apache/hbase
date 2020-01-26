package org.apache.hadoop.hbase.trace;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.samplers.ConstSampler;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class NeverSampler {
  public static Configuration.SamplerConfiguration INSTANCE =
      Configuration.SamplerConfiguration.fromEnv()
          .withType(ConstSampler.TYPE)
          .withParam(0);
}
