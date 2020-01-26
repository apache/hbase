package org.apache.hadoop.hbase.trace;

import io.jaegertracing.Configuration.SamplerConfiguration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class Sampler {

  public static final SamplerConfiguration ALWAYS;
  public static final SamplerConfiguration NEVER;

  static {
    ALWAYS = AlwaysSampler.INSTANCE;
    NEVER = NeverSampler.INSTANCE;
  }
}
