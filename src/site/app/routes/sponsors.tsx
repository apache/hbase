import type { Route } from "./+types/sponsors";
import { SponsorsPage } from "@/pages/sponsors";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "Sponsors - Apache HBase" },
    {
      name: "description",
      content:
        "Companies and organizations that sponsor the Apache HBase project with tools and resources."
    }
  ];
}

export default function Sponsors() {
  return <SponsorsPage />;
}
