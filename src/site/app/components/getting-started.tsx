import { Button } from "@/ui/button";
import { Link } from "react-router";

export function GettingStartedSection() {
  const steps = [
    {
      title: "1. Download",
      desc: "Grab the latest stable release and verify checksums.",
      to: "/downloads"
    },
    {
      title: "2. Read the Guide",
      desc: "Walk through cluster setup, schema design, and operations.",
      to: "https://hbase.apache.org/book.html#_get_started_with_hbase"
    },
    {
      title: "3. Connect a Client",
      desc: "Use the Java API, REST, or Thrift to start building.",
      to: "https://hbase.apache.org/book.html#config.files"
    }
  ];
  return (
    <section id="getting-started" className="border-border/60 bg-muted/30 border-y">
      <div className="container mx-auto px-4 py-12 md:py-16">
        <div className="mb-8 text-center">
          <h2 className="text-3xl font-semibold tracking-tight md:text-4xl">Getting Started</h2>
          <p className="text-muted-foreground mt-2">
            From download to production in a few simple steps.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          {steps.map((s) => (
            <div
              key={s.title}
              className="border-border/60 bg-card flex flex-col rounded-xl border p-5 shadow-sm"
            >
              <h3 className="text-foreground text-lg font-semibold">{s.title}</h3>
              <p className="text-muted-foreground mt-1 text-sm leading-6">{s.desc}</p>
              <div className="mt-auto">
                <Button asChild size="sm" variant="link" className="p-0">
                  <Link to={s.to}>Learn more →</Link>
                </Button>
              </div>
            </div>
          ))}
        </div>
        {/* <div className="mt-8 flex justify-center">
          <Button
            asChild
            size="lg"
            className="bg-primary text-white hover:opacity-90"
          >
            <a href="#download">Download HBase</a>
          </Button>
        </div> */}
      </div>
    </section>
  );
}
