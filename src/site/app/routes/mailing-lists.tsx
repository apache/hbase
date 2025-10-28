import type { Route } from "./+types/mailing-lists";
import { MailingListsPage } from "@/pages/mailing-lists";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "Mailing Lists - Apache HBase" },
    {
      name: "description",
      content:
        "Subscribe to Apache HBase mailing lists including user, developer, commits, issues, and builds lists."
    }
  ];
}

export default function MailingLists() {
  return <MailingListsPage />;
}
