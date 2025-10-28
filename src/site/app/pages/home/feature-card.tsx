import type React from "react";
import { cn } from "@/lib/utils";

export function FeatureCard({
  title,
  children,
  className
}: {
  title: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "border-border/60 bg-card rounded-xl border p-5 text-left shadow-sm",
        className
      )}
    >
      <h3 className="text-primary mb-2 text-lg font-semibold">{title}</h3>
      <p className="text-muted-foreground text-sm leading-6">{children}</p>
    </div>
  );
}
