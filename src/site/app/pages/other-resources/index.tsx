import { BookOpen, ExternalLink } from "lucide-react";
import resources from "./resources.json";

interface Book {
  title: string;
  subtitle: string;
  author: string;
  publisher: string;
  released: string;
  pages: number;
  url: string;
}

interface Resources {
  books: Book[];
}

export function OtherResourcesPage() {
  const data = resources as Resources;

  return (
    <section className="container mx-auto px-4 py-12">
      <h1 className="my-12 text-center text-4xl font-semibold tracking-tight text-balance md:text-6xl">
        Other Apache HBase Resources
      </h1>

      {/* Books Section */}
      {data.books && data.books.length > 0 && (
        <div className="mb-16">
          <h2 className="mb-6 text-3xl font-semibold tracking-tight md:text-4xl">
            Books
          </h2>
          <div className="space-y-4">
            {data.books.map((book, index) => (
              <div
                key={index}
                className="border-border bg-card hover:border-primary/50 group rounded-lg border transition-all hover:shadow-md"
              >
                <div className="border-border flex items-center gap-4 border-b p-5">
                  <div className="bg-primary/10 flex size-12 flex-shrink-0 items-center justify-center rounded-lg">
                    <BookOpen className="text-primary size-6" />
                  </div>
                  <div className="flex-1">
                    <a
                      href={book.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-foreground hover:text-primary group/link inline-flex items-center gap-2 text-xl font-semibold transition-colors"
                    >
                      {book.title}
                      <ExternalLink className="size-4 opacity-0 transition-opacity group-hover:opacity-100" />
                    </a>
                    {book.subtitle && (
                      <p className="text-muted-foreground italic">
                        {book.subtitle}
                      </p>
                    )}
                  </div>
                </div>
                <div className="text-muted-foreground grid grid-cols-1 gap-3 px-5 py-4 text-sm sm:grid-cols-2">
                  <div>
                    <span className="text-foreground font-medium">Author:</span>{" "}
                    {book.author}
                  </div>
                  <div>
                    <span className="text-foreground font-medium">
                      Publisher:
                    </span>{" "}
                    {book.publisher}
                  </div>
                  <div>
                    <span className="text-foreground font-medium">
                      Released:
                    </span>{" "}
                    {book.released}
                  </div>
                  <div>
                    <span className="text-foreground font-medium">Pages:</span>{" "}
                    {book.pages}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
