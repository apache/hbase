import { MarkdownLayout } from "@/components/markdown-layout";
import content from "./content.md?raw";

export function CodeOfConductPage() {
  return <MarkdownLayout>{content}</MarkdownLayout>;
}
