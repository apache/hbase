import { MarkdownLayout } from "@/components/markdown-layout";
import content from "./content.md?raw";

export function TeamPage() {
  return (
    <MarkdownLayout
      components={{
        img: ({ src, alt }) => (
          <img
            src={src || ""}
            alt={alt || ""}
            loading="lazy"
            className="my-0 h-10 w-10 rounded-full"
          />
        ),
        td: ({ children }) => <td className="px-4 py-3 align-middle">{children}</td>
      }}
    >
      {content}
    </MarkdownLayout>
  );
}
