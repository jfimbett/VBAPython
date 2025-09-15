#!/usr/bin/env bash
# Render all Quarto decks to Reveal.js (macOS/Linux)
set -euo pipefail

Q=${1:-quarto}
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p _site

# VBA decks
"$Q" render slides/vba/01-intro-vba.qmd --to revealjs
"$Q" render slides/vba/02-vba-platforms-macros.qmd --to revealjs
"$Q" render slides/vba/03-vba-programming-basics.qmd --to revealjs

# Python decks
"$Q" render slides/python/01-setup-env.qmd --to revealjs
"$Q" render slides/python/02-python-basics.qmd --to revealjs
"$Q" render slides/python/03-oop-types.qmd --to revealjs
"$Q" render slides/python/04-numpy-pandas.qmd --to revealjs
"$Q" render slides/python/05-optimization.qmd --to revealjs
"$Q" render slides/python/06-web-apps.qmd --to revealjs
"$Q" render slides/python/07-bigdata.qmd --to revealjs

# Copy static index
cp -f index.html _site/index.html || true

echo "Render complete. Open _site/index.html"
