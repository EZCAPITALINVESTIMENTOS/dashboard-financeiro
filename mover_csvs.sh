#!/bin/bash
# Move CSVs baixados pelo UP_DIARIO.js de ~/Downloads para as pastas corretas
#
# Uso: ./mover_csvs.sh

DEST=~/Library/Mobile\ Documents/com~apple~CloudDocs/CR\ FINANCIAL\ EZLINK
SRC=~/Downloads

echo "=== Movendo CSVs de ~/Downloads para CR FINANCIAL EZLINK ==="

# Services (check-in indexed)
for f in "$SRC"/CR_Services_20*.csv; do
  [ -f "$f" ] || continue
  # Skip Created files
  case "$f" in *Created*) continue;; esac
  echo "  Services: $(basename "$f")"
  mv "$f" "$DEST/Services/"
done

# Services Created
for f in "$SRC"/CR_Services_Created_*.csv; do
  [ -f "$f" ] || continue
  echo "  Services_Created: $(basename "$f")"
  mv "$f" "$DEST/Services_Created/"
done

# Tickets
for f in "$SRC"/CR_Tickets_*.csv; do
  [ -f "$f" ] || continue
  echo "  Tickets: $(basename "$f")"
  mv "$f" "$DEST/Tickets/"
done

# DebitCredit
for f in "$SRC"/CR_DebitCredit_*.csv; do
  [ -f "$f" ] || continue
  echo "  DebitCredit: $(basename "$f")"
  mv "$f" "$DEST/DebitCredit/"
done

echo "=== Pronto! Agora rode: python3 build_financial_data.py ==="
