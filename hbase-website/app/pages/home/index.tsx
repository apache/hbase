import { Hero } from "@/pages/home/hero";
import { FeaturesSection } from "@/pages/home/features";
import { CommunitySection } from "@/pages/home/community";
import { UseCasesSection } from "@/pages/home/use-cases";

export function HomePage() {
  return (
    <>
      <Hero />
      <FeaturesSection />
      <UseCasesSection />
      <CommunitySection />
    </>
  );
}
