#!/bin/bash
# CR FINANCIAL EZLINK - Atualizacao Completa: Mover CSVs + Rebuild + Deploy
#
# Uso: Apos rodar UP_DIARIO.js no Chrome, execute:
#   ./RODAR_ATUALIZACAO.sh

DIR=~/Library/Mobile\ Documents/com~apple~CloudDocs/CR\ FINANCIAL\ EZLINK
SRC=~/Downloads

cd "$DIR" || { echo "ERRO: diretorio nao encontrado"; exit 1; }

echo "=============================================="
echo "  CR FINANCIAL EZLINK - Atualizacao Completa"
echo "=============================================="

# --- PASSO 1: Mover CSVs de ~/Downloads ---
echo ""
echo "[1/4] Movendo CSVs de ~/Downloads..."
MOVED=0

for f in "$SRC"/CR_Services_20*.csv; do
  [ -f "$f" ] || continue
  case "$f" in *Created*) continue;; esac
  echo "  Services: $(basename "$f")"
  mv "$f" "$DIR/Services/"
  ((MOVED++))
done

for f in "$SRC"/CR_Services_Created_*.csv; do
  [ -f "$f" ] || continue
  echo "  Services_Created: $(basename "$f")"
  mv "$f" "$DIR/Services_Created/"
  ((MOVED++))
done

for f in "$SRC"/CR_Tickets_*.csv; do
  [ -f "$f" ] || continue
  echo "  Tickets: $(basename "$f")"
  mv "$f" "$DIR/Tickets/"
  ((MOVED++))
done

for f in "$SRC"/CR_DebitCredit_*.csv; do
  [ -f "$f" ] || continue
  echo "  DebitCredit: $(basename "$f")"
  mv "$f" "$DIR/DebitCredit/"
  ((MOVED++))
done

echo "  -> $MOVED arquivos movidos"

# --- PASSO 2: Rebuild dashboard ---
echo ""
echo "[2/4] Rodando build_financial_data.py..."
python3 build_financial_data.py
if [ $? -ne 0 ]; then
  echo "ERRO: build falhou!"
  exit 1
fi

# --- PASSO 3: Copiar para index.html ---
echo ""
echo "[3/4] Atualizando index.html..."
cp Dashboard_Financeiro.html index.html
echo "  -> index.html atualizado"

# --- PASSO 4: Deploy no GitHub Pages ---
echo ""
echo "[4/4] Deploy no GitHub..."
git add Dashboard_Financeiro.html index.html
git commit -m "Atualização diária $(date +%Y-%m-%d)"
git push origin main

echo ""
echo "=============================================="
echo "  PRONTO! Dashboard atualizado e publicado"
echo "  https://ezcapitalinvestimentos.github.io/dashboard-financeiro/"
echo "=============================================="
