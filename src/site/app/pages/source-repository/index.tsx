import { MarkdownLayout } from "@/components/markdown-layout";
import content from "./content.md?raw";

export function SourceRepositoryPage() {
  return <MarkdownLayout>{content}</MarkdownLayout>;
}
