import { describe, it, expect, beforeEach } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderWithProviders } from "./utils";
import { SiteNavbar } from "@/components/site-navbar";
import { projectLinks, asfLinks } from "@/components/links";

describe("SiteNavbar", () => {
  beforeEach(() => {
    // Add js class to enable JavaScript-specific menus
    document.documentElement.classList.add("js");
  });

  it("renders the navbar with logo", () => {
    renderWithProviders(<SiteNavbar />);

    const logo = screen.getByAltText(/Apache HBase logo/i);
    expect(logo).toBeInTheDocument();
  });

  it("logo links to home", () => {
    renderWithProviders(<SiteNavbar />);

    const logo = screen.getByAltText(/Apache HBase logo/i);
    const logoLink = logo.closest("a");
    expect(logoLink).toHaveAttribute("href", "/");
  });

  it("displays project menu trigger", () => {
    renderWithProviders(<SiteNavbar />);

    // There are multiple buttons (JS and no-JS), so use getAllByRole
    const projectMenus = screen.getAllByRole("button", {
      name: /Apache HBase Project/i
    });
    expect(projectMenus.length).toBeGreaterThan(0);
  });

  it("displays documentation menu trigger", () => {
    renderWithProviders(<SiteNavbar />);

    // There are multiple buttons (JS and no-JS), so use getAllByRole
    const docsMenus = screen.getAllByRole("button", {
      name: /Documentation and API/i
    });
    expect(docsMenus.length).toBeGreaterThan(0);
  });

  it("displays ASF menu trigger", () => {
    renderWithProviders(<SiteNavbar />);

    // There are multiple buttons (JS and no-JS), so use getAllByRole
    const asfMenus = screen.getAllByRole("button", { name: /ASF/i });
    expect(asfMenus.length).toBeGreaterThan(0);
  });

  it("opens project dropdown menu and shows links", async () => {
    const user = userEvent.setup();
    renderWithProviders(<SiteNavbar />);

    // Get the first project menu button (for desktop JS version)
    const projectMenus = screen.getAllByRole("button", {
      name: /Apache HBase Project/i
    });
    await user.click(projectMenus[0]);

    // Check that first few project links from links.ts are present
    expect(
      screen.getByRole("menuitem", { name: projectLinks[0].label })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("menuitem", { name: projectLinks[1].label })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("menuitem", { name: projectLinks[3].label })
    ).toBeInTheDocument();
  });

  it("opens documentation dropdown menu", async () => {
    const user = userEvent.setup();
    renderWithProviders(<SiteNavbar />);

    const docsMenus = screen.getAllByRole("button", {
      name: /Documentation and API/i
    });
    await user.click(docsMenus[0]);

    // Check for documentation links
    const refGuideLinks = screen.getAllByText(/Reference Guide/i);
    expect(refGuideLinks.length).toBeGreaterThan(0);
  });

  it("opens ASF dropdown menu and shows links", async () => {
    const user = userEvent.setup();
    renderWithProviders(<SiteNavbar />);

    const asfMenus = screen.getAllByRole("button", { name: /ASF/i });
    await user.click(asfMenus[0]);

    // Check first two ASF links from asfLinks
    expect(
      screen.getByRole("menuitem", { name: asfLinks[0].label })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("menuitem", { name: asfLinks[1].label })
    ).toBeInTheDocument();
  });

  it("includes theme toggle", () => {
    renderWithProviders(<SiteNavbar />);

    // Multiple theme toggles (desktop and mobile), so use getAllByRole
    const themeToggles = screen.getAllByRole("button", {
      name: /Toggle theme/i
    });
    expect(themeToggles.length).toBeGreaterThan(0);
  });
});
