import {
  ChartNoAxesCombined,
  Dna,
  Radio,
  ScrollText,
  ShieldCheck,
  Target
} from "lucide-react";

export function UseCasesSection() {
  const items = [
    {
      title: "Time-Series & Metrics",
      desc: "High-ingest, append-mostly workloads with predictable access patterns.",
      Icon: ChartNoAxesCombined
    },
    {
      title: "IoT Telemetry",
      desc: "Massive device streams stored by time and entity with scalable reads.",
      Icon: Radio
    },
    {
      title: "AdTech & Personalization",
      desc: "Low-latency serving of profiles, events, and counters at scale.",
      Icon: Target
    },
    {
      title: "Message & Audit Logs",
      desc: "Immutable logs and audit trails with efficient range scans.",
      Icon: ScrollText
    },
    {
      title: "Security Analytics",
      desc: "Store and query security events for investigation and alerting.",
      Icon: ShieldCheck
    },
    {
      title: "Genomics & Research",
      desc: "Columnar, sparse data sets with large key spaces.",
      Icon: Dna
    }
  ];
  return (
    <section id="use-cases" className="border-border/60 bg-muted/30 border-y">
      <div className="container mx-auto px-4 py-12 md:py-16">
        <div className="mb-8 text-center">
          <h2 className="text-3xl font-semibold tracking-tight md:text-4xl">
            Use Cases
          </h2>
          <p className="text-muted-foreground mt-2">
            Battle-tested patterns where HBase excels.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {items.map(({ title, desc, Icon }) => (
            <div
              key={title}
              className="group border-border/60 bg-card rounded-xl border p-5 shadow-sm"
            >
              <div className="flex items-start gap-2">
                <Icon
                  className="text-primary mt-0.5 size-[26px] shrink-0"
                  aria-hidden
                />
                <div>
                  <h3 className="text-foreground text-lg font-semibold">
                    {title}
                  </h3>
                  <p className="text-muted-foreground mt-1 text-sm leading-6">
                    {desc}
                  </p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
