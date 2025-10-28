import { describe, it, expect, beforeEach } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderWithProviders } from "./utils";
import { ThemeToggle } from "@/components/theme-toggle";

describe("ThemeToggle", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("renders the theme toggle button", () => {
    renderWithProviders(<ThemeToggle />);

    const button = screen.getByRole("button", { name: /Toggle theme/i });
    expect(button).toBeInTheDocument();
  });

  it("toggles theme when clicked", async () => {
    const user = userEvent.setup();
    renderWithProviders(<ThemeToggle />);

    const button = screen.getByRole("button", { name: /Toggle theme/i });

    // Initially should be light theme (default)
    expect(document.documentElement.classList.contains("dark")).toBe(false);

    // Click to toggle to dark
    await user.click(button);
    expect(document.documentElement.classList.contains("dark")).toBe(true);

    // Click to toggle back to light
    await user.click(button);
    expect(document.documentElement.classList.contains("dark")).toBe(false);
  });

  it("has sun and moon icons", () => {
    renderWithProviders(<ThemeToggle />);

    // Both icons should be in the DOM (one visible, one hidden based on theme)
    const button = screen.getByRole("button", { name: /Toggle theme/i });
    expect(button).toBeInTheDocument();

    // Check SVG elements exist (lucide-react renders as svg)
    const svgs = button.querySelectorAll("svg");
    expect(svgs.length).toBe(2); // Sun and Moon icons
  });
});
