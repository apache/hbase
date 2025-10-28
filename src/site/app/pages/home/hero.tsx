import { Button } from "@/ui/button";
import { Link } from "react-router";

export function Hero() {
  return (
    <section>
      <div className="container mx-auto px-4 pt-32 pb-16 md:pt-48 md:pb-20">
        <div className="flex flex-col items-center text-center">
          <img
            src="/images/large-logo.svg"
            alt="Apache HBase logo"
            width={450}
            height={115}
            className="px-4 sm:px-0 dark:hidden"
          />
          <img
            src="/images/dark-theme-large-logo.svg"
            alt="Apache HBase logo"
            width={450}
            height={115}
            className="hidden px-4 sm:px-0 dark:block"
          />

          <h1 className="mt-6 text-4xl font-semibold tracking-tight text-balance md:text-6xl">
            The Hadoop Database
          </h1>
          <p className="text-muted-foreground mt-4 max-w-2xl text-lg text-pretty md:text-xl">
            A distributed, scalable, big data store for random, realtime
            read/write access.
          </p>

          <div className="mt-8 flex flex-wrap items-center justify-center gap-3">
            <Button asChild size="lg">
              <Link to="/downloads">Download HBase</Link>
            </Button>
            <Button asChild size="lg" variant="outline">
              <Link to="https://hbase.apache.org/book.html">
                Read Documentation
              </Link>
            </Button>
          </div>
        </div>
      </div>
    </section>
  );
}
