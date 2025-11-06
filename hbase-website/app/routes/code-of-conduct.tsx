import type { Route } from "./+types/code-of-conduct";
import { CodeOfConductPage } from "@/pages/code-of-conduct";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "Code of Conduct - Apache HBase" },
    {
      name: "description",
      content: "Code of Conduct and Diversity Statement for the Apache HBase project community."
    }
  ];
}

export default function CodeOfConduct() {
  return <CodeOfConductPage />;
}
