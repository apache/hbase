import type { Route } from "./+types/powered-by-hbase";
import { PoweredByHBasePage } from "@/pages/powered-by-hbase";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "Powered by HBase - Apache HBase" },
    {
      name: "description",
      content:
        "Companies and organizations using Apache HBase in production for their data storage and processing needs."
    }
  ];
}

export default function PoweredByHBase() {
  return <PoweredByHBasePage />;
}
