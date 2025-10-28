import type { Route } from "./+types/acid-semantics";
import { AcidSemanticsPage } from "@/pages/acid-semantics";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "ACID Semantics - Apache HBase" },
    {
      name: "description",
      content: "Apache HBase ACID properties and guarantees specification."
    }
  ];
}

export default function AcidSemantics() {
  return <AcidSemanticsPage />;
}
