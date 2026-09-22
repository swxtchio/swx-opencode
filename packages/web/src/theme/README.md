# Vendored docs theme

Vendored from `toolbeam-docs-theme@0.4.8` (MIT — see `LICENSE`).

## Why it is vendored rather than a dependency

The published package declares `peerDependencies` of `astro ^5.7.13` and
`@astrojs/starlight ^0.34.3`, and 0.4.8 is the newest release. Depending on it
while the site runs Astro 7 / Starlight 0.42 made bun install a second, private
copy of `astro@5.7.13` and `@astrojs/starlight@0.34.3` to satisfy those peers.
That nested copy kept a **critical** advisory in the tree
(GHSA-26w7-cxv4-gfx2, RCE via AVIF image optimization, fixed in 7.2.8), even
though the site itself was on a patched Astro.

Vendoring the sources drops the stale peer contract, so only one Astro is
installed.

## Local changes against upstream 0.4.8

- Bare `toolbeam-docs-theme/...` specifiers rewritten to the vendored paths in
  `index.ts` and `src/lib/starlight.ts`.
- `styles/theme.css`: Starlight 0.42 replaced `<starlight-menu-button>` and its
  `aria-expanded` state with a plain `<button class="sl-menu-button"
  popovertarget>` whose open state is expressed as `:popover-open`. The mobile
  nav button rules were retargeted accordingly; the old selectors matched
  nothing after the upgrade.
