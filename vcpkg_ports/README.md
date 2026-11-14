# GNU libiconv overlay

`libiconv/` is copied from
`intellekthq/duckdb-extension-ci-tools@c09374aa16e61d7fd6fbc13d8dc3ffc498d60914`.
The port originates in Microsoft's vcpkg (MIT, see `LICENSE.vcpkg`); its patch
builds GNU libiconv on macOS instead of accepting the system stub. The port's
copyright-install step preserves GNU libiconv's LGPL/GPL notices.

Keep this overlay in the extension repository: community CI replaces the
`extension-ci-tools` checkout with the upstream harness, which has no libiconv
overlay. `vcpkg.json` lists this directory before the CI-tools manifest overlay.
The source manifest takes precedence over the harness's environment overlay,
so the source-owned libiconv port must appear in vcpkg's install plan. CMake
rejects built-in iconv on macOS, and
`test/sql/utf16_decoding.test` checks known non-ASCII UTF-16LE fixture text.
