import type { Route } from "./+types/other-resources";
import { OtherResourcesPage } from "@/pages/other-resources";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "Other Resources - Apache HBase" },
    {
      name: "description",
      content: "Books and other learning resources about Apache HBase."
    }
  ];
}

export default function OtherResources() {
  return <OtherResourcesPage />;
}
