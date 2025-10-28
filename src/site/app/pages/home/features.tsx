import { FeatureCard } from "@/pages/home/feature-card";

export function FeaturesSection() {
  const features = [
    {
      title: "Billions of Rows",
      desc: "Host massive tables with billions of rows and millions of columns on commodity clusters."
    },
    {
      title: "Real-time Access",
      desc: "Get random, realtime read/write access with strictly consistent operations."
    },
    {
      title: "Built on Hadoop",
      desc: "Bigtable-like capabilities on top of Hadoop and HDFS with automatic failover and sharding."
    },
    {
      title: "Flexible APIs",
      desc: "Java client, REST and Thrift gateways, server-side filters, Bloom filters, and more."
    }
  ];
  return (
    <section id="features" className="container mx-auto px-4 py-12 md:py-16">
      <div className="mb-8 text-center">
        <h2 className="text-3xl font-semibold tracking-tight md:text-4xl">Why HBase</h2>
        <p className="text-muted-foreground mt-2">
          Linearly scalable, consistent, and proven for large-scale workloads.
        </p>
      </div>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {features.map((f) => (
          <FeatureCard key={f.title} title={f.title}>
            {f.desc}
          </FeatureCard>
        ))}
      </div>
    </section>
  );
}
