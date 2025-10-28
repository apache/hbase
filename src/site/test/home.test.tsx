import { describe, it, expect } from "vitest";
import { screen } from "@testing-library/react";
import { renderWithProviders } from "./utils";
import { HomePage } from "@/pages/home";

describe("HomePage", () => {
  it("renders the hero section with main heading", () => {
    renderWithProviders(<HomePage />);

    const heading = screen.getByRole("heading", {
      name: /The Hadoop Database/i
    });
    expect(heading).toBeInTheDocument();
  });

  it("displays the Apache HBase logo", () => {
    renderWithProviders(<HomePage />);

    const logos = screen.getAllByAltText(/Apache HBase logo/i);
    expect(logos.length).toBeGreaterThan(0);
  });

  it("shows the main description text", () => {
    renderWithProviders(<HomePage />);

    const description = screen.getByText(
      /A distributed, scalable, big data store/i
    );
    expect(description).toBeInTheDocument();
  });

  it("displays Download HBase button", () => {
    renderWithProviders(<HomePage />);

    const downloadButton = screen.getByRole("link", {
      name: /Download HBase/i
    });
    expect(downloadButton).toBeInTheDocument();
    expect(downloadButton).toHaveAttribute("href", "/downloads");
  });

  it("displays Read Documentation button", () => {
    renderWithProviders(<HomePage />);

    const docsButton = screen.getByRole("link", {
      name: /Read Documentation/i
    });
    expect(docsButton).toBeInTheDocument();
  });

  it("renders the features section", () => {
    renderWithProviders(<HomePage />);

    // Check for key feature headings
    expect(screen.getByText("Billions of Rows")).toBeInTheDocument();
    expect(screen.getByText("Real-time Access")).toBeInTheDocument();
  });

  it("renders use cases section", () => {
    renderWithProviders(<HomePage />);

    // Check for use cases section by ID
    const useCases = document.querySelector("#use-cases");
    expect(useCases).toBeInTheDocument();
  });

  it("renders community section", () => {
    renderWithProviders(<HomePage />);

    // Look for a heading specific to the community section
    expect(
      screen.getByRole("heading", { name: /Vibrant Community/i })
    ).toBeInTheDocument();
  });
});
