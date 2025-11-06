import { render } from "@testing-library/react";
import { BrowserRouter } from "react-router";
import { ThemeProvider } from "@/lib/theme-provider";
import type { ReactElement } from "react";

export function renderWithProviders(ui: ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider defaultTheme="light" storageKey="test-theme">
        {ui}
      </ThemeProvider>
    </BrowserRouter>
  );
}

export * from "@testing-library/react";
