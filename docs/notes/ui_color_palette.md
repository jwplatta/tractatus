---
date_created: 2026-10-26
description: Role-based color palette for Tractatus UI (coverage app and future dashboards), with light and dark mode values.
pull_request: https://github.com/jwplatta/tractatus/pull/30
---

# UI Color Palette

Role-based palette used in the lakehouse coverage app and any future Tractatus dashboards. Defined in `.streamlit/config.toml` using `[theme.light]` / `[theme.dark]` sections, which preserves Streamlit's native System / Light / Dark toggle.

## Roles and values

| Role | Light mode | Dark mode |
|---|---|---|
| Page background | `#F9FAFB` | `#0B1220` |
| Cards and panels | `#FFFFFF` | `#111827` |
| Primary blue | `#1D4ED8` | `#60A5FA` |
| Teal accent | `#0D9488` | `#2DD4BF` |
| Main text | `#111827` | `#F9FAFB` |
| Secondary text | `#4B5563` | `#9CA3AF` |
| Borders | `#D1D5DB` | `#374151` |

## Usage rules

- **Blue** is the action color: buttons, links, selected states.
  - Light mode button text: `#FFFFFF`; dark mode button text: `#0B1220` (high contrast pairs).
- **Teal** is for charts and occasional highlights — keeps blue as the clear action color.
- **Heatmap colorscale** (coverage app):
  - Weekend: `#6B7280` (mid gray, readable on both backgrounds)
  - No data (past weekday): `#FECACA` (light red)
  - Future date: `#F8FAFC` (near white / blank)
  - Data — fewer rows: `#2DD4BF` (bright teal)
  - Data — more rows: `#0d4f4a` (dark teal)
  - z-scale: `zmin=-1`, `zmax=1`; colorscale positions = `(z + 1) / 2`

## Streamlit config

Defined in `.streamlit/config.toml`. Using `[theme.light]` and `[theme.dark]` (not `[theme]`) is required to keep the System/Light/Dark toggle visible in Streamlit v1.66+.
