import type { Route } from "./+types/home";
import { HomePage } from "@/pages/home";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "Apache HBase" },
    {
      name: "description",
      content:
        "Apache HBase® is the Hadoop database, a distributed, scalable, big data store."
    }
  ];
}

export default function Home() {
  return <HomePage />;
}
