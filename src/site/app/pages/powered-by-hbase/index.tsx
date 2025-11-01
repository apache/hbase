import { Building2, ExternalLink } from "lucide-react";
import companies from "./companies.json";

interface Company {
  name: string;
  url: string;
  description: string;
}

export function PoweredByHBasePage() {
  return (
    <div className="container mx-auto px-4 py-12">
      <article className="prose prose-slate dark:prose-invert mx-auto">
        <h1 className="my-12 text-center text-4xl font-semibold tracking-tight text-balance md:text-6xl">
          Powered By Apache HBase™
        </h1>

        <p className="text-muted-foreground mb-8 text-center text-base leading-7">
          This page lists some institutions and projects which are using HBase. To have your
          organization added, file a documentation JIRA or email{" "}
          <a
            href="mailto:dev@hbase.apache.org"
            className="text-primary underline-offset-4 hover:underline"
          >
            hbase-dev
          </a>{" "}
          with the relevant information. If you notice out-of-date information, use the same avenues
          to report it.
        </p>

        <p className="mb-12 text-center text-sm font-semibold">
          These items are user-submitted and the HBase team assumes no responsibility for their
          accuracy.
        </p>

        <div className="space-y-4">
          {(companies as Company[]).map((company) => (
            <div
              key={company.name}
              className="border-border bg-card hover:border-primary/50 group rounded-lg border transition-all hover:shadow-md"
            >
              <div className="border-border flex items-center gap-3 border-b p-5">
                <div className="bg-primary/10 flex size-10 flex-shrink-0 items-center justify-center rounded-lg">
                  <Building2 className="text-primary size-5" />
                </div>
                <div className="flex-1">
                  <a
                    href={company.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-foreground hover:text-primary inline-flex items-center gap-2 text-xl font-semibold transition-colors"
                  >
                    {company.name}
                    <ExternalLink className="size-4 opacity-0 transition-opacity group-hover:opacity-100" />
                  </a>
                </div>
              </div>
              <div className="text-muted-foreground px-5 py-4 text-sm leading-relaxed">
                {company.description}
              </div>
            </div>
          ))}
        </div>
      </article>
    </div>
  );
}
