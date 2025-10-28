import { MarkdownLayout } from "@/components/markdown-layout";
import content from "./content.md?raw";

export function AcidSemanticsPage() {
  return <MarkdownLayout>{content}</MarkdownLayout>;
}
