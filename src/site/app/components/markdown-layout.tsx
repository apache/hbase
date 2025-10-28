import Markdown, { type Components } from "react-markdown";
import { Link } from "react-router";
import remarkGfm from "remark-gfm";
import rehypeSlug from "rehype-slug";
import rehypeAutolinkHeadings from "rehype-autolink-headings";
import rehypeHighlight from "rehype-highlight";
import rehypeRaw from "rehype-raw";
import { ExternalLinkIcon } from "lucide-react";

interface MarkdownLayoutProps {
  children: string;
  autoLinkHeadings?: boolean;
  highlight?: boolean;
  components?: Components;
}

export function MarkdownLayout({
  children,
  autoLinkHeadings = false,
  highlight = true,
  components: customComponents
}: MarkdownLayoutProps) {
  const rehypePlugins: any[] = [rehypeRaw];
  if (autoLinkHeadings) {
    rehypePlugins.push(rehypeSlug, [
      rehypeAutolinkHeadings,
      { behavior: "wrap" }
    ]);
  }
  if (highlight) {
    // ignoreMissing avoids errors if someone writes `language-xyz` you don't ship
    rehypePlugins.push([rehypeHighlight, { ignoreMissing: true }]);
  }

  return (
    <div className="container mx-auto px-4 py-12">
      <article className="prose prose-slate dark:prose-invert">
        <Markdown
          remarkPlugins={[remarkGfm]}
          rehypePlugins={rehypePlugins}
          components={{
            h1: ({ children }) => (
              <h1 className="my-12 text-center text-4xl font-semibold tracking-tight text-balance md:text-6xl">
                {children}
              </h1>
            ),
            h2: ({ children }) => (
              <h2 className="mt-12 mb-4 scroll-mt-28 text-3xl font-semibold tracking-tight md:text-4xl">
                {children}
              </h2>
            ),
            h3: ({ children }) => (
              <h3 className="mt-8 mb-1 scroll-mt-28 text-xl font-semibold tracking-tight">
                {children}
              </h3>
            ),
            p: ({ children }) => (
              <p className="mb-4 text-base leading-7">{children}</p>
            ),
            a: ({ href, children }) => {
              const isExternal =
                href?.startsWith("http") &&
                !href?.startsWith("https://hbase.apache.org/");

              // Check if the link contains only an image (no external icon needed)
              const hasOnlyImage = Array.isArray(children)
                ? children.every(
                    (child) =>
                      child?.type === "img" || typeof child === "object"
                  )
                : typeof children === "object";

              return isExternal ? (
                <a
                  href={href ?? "#"}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={
                    hasOnlyImage
                      ? "inline-block"
                      : "text-primary inline underline-offset-4 hover:underline"
                  }
                >
                  {children}
                  {!hasOnlyImage && (
                    <>
                      {"\u00A0"}
                      <ExternalLinkIcon className="inline size-3.5 align-[-2px]" />
                    </>
                  )}
                </a>
              ) : (
                <Link
                  to={href ?? "#"}
                  className="text-primary underline-offset-4 hover:underline"
                >
                  {children}
                </Link>
              );
            },
            ol: ({ children }) => (
              <ol className="mb-4 ml-6 list-decimal space-y-2">{children}</ol>
            ),
            ul: ({ children }) => (
              <ul className="mb-4 ml-6 list-disc space-y-2">{children}</ul>
            ),
            li: ({ children }) => <li className="leading-7">{children}</li>,
            // Keep code/pre lean so highlight.js classes (`hljs ...`) can style properly
            code: ({ children, className }) => {
              const isInline = !className;
              if (isInline) {
                return (
                  <code className="bg-muted rounded px-1.5 py-0.5 font-mono text-sm">
                    {children}
                  </code>
                );
              }
              return (
                <code
                  className={`${className} block rounded font-mono text-sm`}
                >
                  {children}
                </code>
              );
            },
            pre: ({ children }) => (
              <pre className="bg-muted mb-4 overflow-x-auto rounded-lg p-4">
                {children}
              </pre>
            ),
            blockquote: ({ children }) => (
              <blockquote className="border-border my-6 border-l-4 pl-6 italic">
                {children}
              </blockquote>
            ),
            img: ({ src, alt }) => (
              <img
                src={src || ""}
                alt={alt || ""}
                loading="lazy"
                className="my-6 max-w-full rounded-lg"
              />
            ),
            table: ({ children }) => (
              <div className="border-border my-8 w-full overflow-x-auto rounded-lg border">
                <table className="w-full border-collapse text-sm">
                  {children}
                </table>
              </div>
            ),
            thead: ({ children }) => (
              <thead className="bg-muted">{children}</thead>
            ),
            tbody: ({ children }) => <tbody>{children}</tbody>,
            tr: ({ children }) => (
              <tr className="border-border hover:bg-muted/50 border-b transition-colors">
                {children}
              </tr>
            ),
            th: ({ children }) => (
              <th className="px-4 py-3 text-left font-semibold">{children}</th>
            ),
            td: ({ children }) => (
              <td className="px-4 py-3 align-top">{children}</td>
            ),
            ...customComponents
          }}
        >
          {children}
        </Markdown>
      </article>
    </div>
  );
}
