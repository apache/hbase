import { MarkdownLayout } from "@/components/markdown-layout";
import content from "./content.md?raw";

export function DownloadsPage() {
  return <MarkdownLayout>{content}</MarkdownLayout>;
}
