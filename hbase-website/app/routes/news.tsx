import type { Route } from "./+types/news";
import { NewsPage } from "@/pages/news";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "News - Apache HBase" },
    {
      name: "description",
      content: "Latest news, events, and releases for Apache HBase."
    }
  ];
}

export default function News() {
  return <NewsPage />;
}
