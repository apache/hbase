import { MarkdownLayout } from "@/components/markdown-layout";
import content from "./content.md?raw";

export function SponsorsPage() {
  return <MarkdownLayout>{content}</MarkdownLayout>;
}
