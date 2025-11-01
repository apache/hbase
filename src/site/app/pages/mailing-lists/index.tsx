import { MarkdownLayout } from "@/components/markdown-layout";
import content from "./content.md?raw";

export function MailingListsPage() {
  return <MarkdownLayout>{content}</MarkdownLayout>;
}
