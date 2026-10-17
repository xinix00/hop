# Shared UI assets

Vendored from the sibling Haasstyle project (`/Users/derek/Git/haasstyle`) on 2026-09-22, matching the components used by EasyACP:

- `tactile-theme.css`: shared tokens, including the stepped neutral light palette.
- `tactile-elements.css`, `tactile-components.css`: controls, panels, tables, dialogs and spinner.
- `tactile95-material.css`, `tactile-matte-material.css`: scoped material variants selected by `data-style`.
- `tactile-select.js`, `tactile-dialog.js`: shared interaction behavior.
- `material-symbols-outlined.css` and `.woff2`: locally hosted Material Symbols icons, under the included Apache 2.0 license (`LICENSE.material-symbols`).

The Tactile files are unchanged copies. Update them together from Haasstyle and verify all three styles in both modes. Hop-specific composition belongs in `../style.css`. No files are fetched from a CDN at runtime.
