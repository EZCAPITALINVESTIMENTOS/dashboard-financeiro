#!/usr/bin/env python3
"""
build_financial_data.py — CR FINANCIAL EZLINK Dashboard Builder

Reads Services, DebitCredit, Tickets CSVs and aggregates financial metrics:
Cash Flow, AR/AP, P&L, Margin, Currency, Invoicing, DSO.

Usage:
    python3 build_financial_data.py
"""

import csv
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, date

# --- CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_FILE = os.path.join(BASE_DIR, "Dashboard_Financeiro.html")
SERVICES_DIR = os.path.join(BASE_DIR, "Services")
SERVICES_CREATED_DIR = os.path.join(BASE_DIR, "Services_Created")
TICKETS_DIR = os.path.join(BASE_DIR, "Tickets")
DEBITCREDIT_DIR = os.path.join(BASE_DIR, "DebitCredit")

TODAY = date.today().isoformat()


# --- HELPERS ---

def parse_date(s):
    if not s or not isinstance(s, str):
        return None
    s = s.strip().strip('"')
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_float(s):
    if not s or not isinstance(s, str):
        return 0.0
    s = s.strip().strip('"')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def parse_int(s):
    if not s or not isinstance(s, str):
        return 0
    s = s.strip().strip('"')
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def month_key(dt):
    if dt is None:
        return None
    return dt.strftime("%Y-%m")


def date_key(dt):
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def status_code(status_str):
    sl = (status_str or "").lower()
    if "confirmed" in sl:
        return "C"
    elif "cancelled" in sl or "canceled" in sl:
        return "X"
    elif "rejected" in sl:
        return "R"
    elif "in progress" in sl:
        return "I"
    return "C"


def r2(v):
    return round(v, 2)


# --- AI INSIGHTS ENGINE ---

def generate_insights(data):
    """
    Auto-generative financial intelligence engine.
    Analyzes all aggregated data and produces categorized insights:
    - alerts: critical issues requiring immediate attention
    - trends: notable changes in key metrics
    - opportunities: areas for improvement
    - summary: executive summary bullets
    """
    insights = []

    def add(category, severity, title, detail, metric=None):
        insights.append({
            "cat": category,  # alert, trend, opportunity, summary
            "sev": severity,  # critical, warning, info, positive
            "title": title,
            "detail": detail,
            "metric": metric,
        })

    pnl = data.get("pnl_monthly", {})
    months = sorted(pnl.keys())
    mt = data.get("margin_monthly_trend", {})
    canc = data.get("cancellation_monthly", {})
    cf = data.get("cashflow_monthly", {})
    inv_s = data.get("invoice_summary", {})
    neg = data.get("negative_margin_services", [])
    ar = data.get("ar_by_agency", {})
    aging = data.get("ar_aging", {})
    dso_ag = data.get("dso_by_agency", {})
    ref_ag = data.get("refund_by_agency", {})
    ma = data.get("margin_by_agency", {})
    mp = data.get("margin_by_provider", {})
    upcoming = data.get("upcoming_checkins", {})

    # --- EXECUTIVE SUMMARY ---
    if months:
        total_rev = sum(pnl[m].get("revenue_gross", 0) for m in months)
        total_prof = sum(pnl[m].get("gross_profit", 0) for m in months)
        total_net = sum(pnl[m].get("net_operating", 0) for m in months)
        overall_margin = (total_prof / total_rev * 100) if total_rev > 0 else 0

        last3 = months[-3:]
        rev_last3 = sum(pnl[m].get("revenue_gross", 0) for m in last3)
        prof_last3 = sum(pnl[m].get("gross_profit", 0) for m in last3)

        add("summary", "info", "Volume Total",
            f"R$ {total_rev:,.0f} em revenue total com {data['metadata']['total_services']:,} servicos processados.",
            f"R$ {total_rev:,.0f}")

        add("summary", "positive" if overall_margin > 3 else "warning",
            "Margem Geral",
            f"Margem bruta de {overall_margin:.1f}% sobre todo o periodo. "
            f"Profit total: R$ {total_prof:,.0f}.",
            f"{overall_margin:.1f}%")

        add("summary", "info", "Pipeline Futuro",
            f"{sum(v.get('n',0) for v in upcoming.values()):,} reservas confirmadas para os proximos meses, "
            f"totalizando R$ {sum(v.get('value',0) for v in upcoming.values()):,.0f}.")

    # --- MARGIN TREND ANALYSIS ---
    mt_months = sorted(mt.keys())
    if len(mt_months) >= 3:
        last3_margins = [mt[m].get("margin_pct", 0) for m in mt_months[-3:]]
        prev3_margins = [mt[m].get("margin_pct", 0) for m in mt_months[-6:-3]] if len(mt_months) >= 6 else []

        avg_last3 = sum(last3_margins) / 3
        avg_prev3 = sum(prev3_margins) / 3 if prev3_margins else avg_last3

        if avg_last3 < avg_prev3 * 0.85:
            add("alert", "critical", "Margem em Queda",
                f"Margem media caiu de {avg_prev3:.1f}% para {avg_last3:.1f}% "
                f"nos ultimos 3 meses (-{((avg_prev3-avg_last3)/avg_prev3*100):.0f}%).",
                f"{avg_last3:.1f}%")
        elif avg_last3 > avg_prev3 * 1.1:
            add("trend", "positive", "Margem em Alta",
                f"Margem media subiu de {avg_prev3:.1f}% para {avg_last3:.1f}% "
                f"nos ultimos 3 meses (+{((avg_last3-avg_prev3)/avg_prev3*100):.0f}%).",
                f"{avg_last3:.1f}%")

        # Markup trend
        last3_mkp = [mt[m].get("mkp_avg", 0) for m in mt_months[-3:]]
        avg_mkp = sum(last3_mkp) / 3
        if avg_mkp < 3.0:
            add("alert", "warning", "Markup Medio Baixo",
                f"Markup medio dos ultimos 3 meses e {avg_mkp:.1f}%. "
                f"Considere rever regras no EZConnect para integracao com markup < 3%.",
                f"{avg_mkp:.1f}%")

    # --- CANCELLATION ANALYSIS ---
    canc_months = sorted(canc.keys())
    if canc_months:
        last3_canc = canc_months[-3:]
        canc_total = sum(canc.get(m, {}).get("n", 0) for m in last3_canc)
        canc_lost = sum(canc.get(m, {}).get("lost_rev", 0) for m in last3_canc)
        total_n = sum(pnl.get(m, {}).get("n_total", 0) for m in last3_canc)
        canc_rate = (canc_total / total_n * 100) if total_n > 0 else 0

        if canc_rate > 15:
            add("alert", "critical", "Taxa de Cancelamento Alta",
                f"Taxa de cancelamento de {canc_rate:.1f}% nos ultimos 3 meses. "
                f"Receita perdida: R$ {canc_lost:,.0f}.",
                f"{canc_rate:.1f}%")
        elif canc_rate > 8:
            add("alert", "warning", "Cancelamentos Acima da Media",
                f"Taxa de cancelamento de {canc_rate:.1f}% nos ultimos 3 meses "
                f"({canc_total:,} cancelamentos, R$ {canc_lost:,.0f} perdidos).",
                f"{canc_rate:.1f}%")

    # --- NEGATIVE MARGIN ALERT ---
    if len(neg) > 50:
        total_loss = sum(n.get("loss", 0) for n in neg)
        add("alert", "critical", f"{len(neg)} Servicos com Margem Negativa",
            f"Perda total de R$ {abs(total_loss):,.0f} em servicos confirmados com margem negativa. "
            f"Verificar precificacao e markup por provider/hotel.",
            f"R$ {abs(total_loss):,.0f}")
    elif len(neg) > 10:
        total_loss = sum(n.get("loss", 0) for n in neg)
        add("alert", "warning", f"{len(neg)} Servicos com Margem Negativa",
            f"Perda de R$ {abs(total_loss):,.0f}. Revisar regras de markup.",
            f"R$ {abs(total_loss):,.0f}")

    # --- INVOICE / NF COMPLIANCE ---
    inv_pct = inv_s.get("invoiced_pct", 0)
    nf_pct = inv_s.get("nf_pct", 0)
    if nf_pct < 50:
        add("alert", "critical", "NF Emissao Baixa",
            f"Apenas {nf_pct:.0f}% dos registros DC tem nota fiscal emitida. "
            f"Risco fiscal - verificar processo de emissao de NF.",
            f"{nf_pct:.0f}%")
    elif nf_pct < 80:
        add("alert", "warning", "NF Abaixo do Ideal",
            f"{nf_pct:.0f}% dos registros tem NF. Meta: >90%.",
            f"{nf_pct:.0f}%")

    if inv_pct > 85:
        add("trend", "positive", "Faturamento Saudavel",
            f"{inv_pct:.0f}% dos registros estao faturados.", f"{inv_pct:.0f}%")

    # --- AR AGING ALERTS ---
    aging_120 = aging.get("120d+", {}).get("amount", 0)
    aging_90 = aging.get("90d", {}).get("amount", 0)
    if aging_120 > 100000:
        add("alert", "critical", "AR Vencido 120+ dias",
            f"R$ {aging_120:,.0f} em contas a receber com mais de 120 dias. "
            f"Alto risco de inadimplencia.",
            f"R$ {aging_120:,.0f}")
    if aging_90 + aging_120 > 200000:
        add("alert", "warning", "AR Vencido 90+ dias",
            f"R$ {(aging_90 + aging_120):,.0f} em contas vencidas ha mais de 90 dias.",
            f"R$ {(aging_90+aging_120):,.0f}")

    # --- DSO OUTLIERS ---
    dso_vals = [(ag, v.get("avg_dso", 0)) for ag, v in dso_ag.items() if v.get("n", 0) >= 5]
    if dso_vals:
        dso_vals.sort(key=lambda x: x[1], reverse=True)
        worst3 = dso_vals[:3]
        overall_dso = sum(v for _, v in dso_vals) / len(dso_vals)
        for ag, dso in worst3:
            if dso > overall_dso * 2 and dso > 60:
                add("alert", "warning", f"DSO Alto: {ag}",
                    f"DSO de {dso:.0f} dias (media geral: {overall_dso:.0f} dias). "
                    f"Cobrar pagamento ou rever condicoes.",
                    f"{dso:.0f} dias")

    # --- REFUND CONCENTRATION ---
    ref_items = list(ref_ag.items())
    if ref_items:
        ref_items.sort(key=lambda x: x[1].get("ref_c", 0), reverse=True)
        top_ref = ref_items[0]
        if top_ref[1].get("ref_c", 0) > 50000:
            add("alert", "warning", f"Refunds Concentrados: {top_ref[0]}",
                f"R$ {top_ref[1]['ref_c']:,.0f} em refunds para cliente. "
                f"Investigar causa raiz.",
                f"R$ {top_ref[1]['ref_c']:,.0f}")

    # --- REVENUE GROWTH ---
    if len(months) >= 6:
        rev_recent = [pnl[m].get("revenue_gross", 0) for m in months[-3:]]
        rev_prior = [pnl[m].get("revenue_gross", 0) for m in months[-6:-3]]
        avg_recent = sum(rev_recent) / 3
        avg_prior = sum(rev_prior) / 3
        if avg_prior > 0:
            growth = (avg_recent - avg_prior) / avg_prior * 100
            if growth > 15:
                add("trend", "positive", "Revenue em Crescimento",
                    f"Revenue medio mensal cresceu {growth:.0f}% (R$ {avg_prior:,.0f} -> R$ {avg_recent:,.0f}).",
                    f"+{growth:.0f}%")
            elif growth < -15:
                add("trend", "warning", "Revenue em Queda",
                    f"Revenue medio mensal caiu {abs(growth):.0f}% (R$ {avg_prior:,.0f} -> R$ {avg_recent:,.0f}).",
                    f"{growth:.0f}%")

    # --- PROVIDER MARGIN OPPORTUNITIES ---
    mp_items = list(mp.items())
    if mp_items:
        low_margin_providers = [(p, v) for p, v in mp_items
                                if v.get("margin", 0) < 2 and v.get("n", 0) > 100]
        for p, v in sorted(low_margin_providers, key=lambda x: x[1]["rev"], reverse=True)[:3]:
            add("opportunity", "info", f"Margem Baixa: {p}",
                f"Provider com {v['n']:,} servicos e apenas {v['margin']:.1f}% de margem "
                f"(R$ {v['rev']:,.0f} revenue). Negociar melhores condicoes ou aumentar markup.",
                f"{v['margin']:.1f}%")

    # --- AGENCY CONCENTRATION ---
    ma_items = sorted(ma.items(), key=lambda x: x[1].get("rev", 0), reverse=True)
    if ma_items and total_rev > 0:
        top5_rev = sum(v.get("rev", 0) for _, v in ma_items[:5])
        concentration = top5_rev / total_rev * 100
        if concentration > 60:
            top5_names = ", ".join(a for a, _ in ma_items[:5])
            add("alert", "warning", "Alta Concentracao de Receita",
                f"Top 5 agencias representam {concentration:.0f}% da receita total: {top5_names}. "
                f"Diversificar base de clientes.",
                f"{concentration:.0f}%")

    # --- CASH FLOW HEALTH ---
    if len(months) >= 3:
        last3_net = [cf.get(m, {}).get("net_brl", 0) for m in months[-3:]]
        negative_months = sum(1 for n in last3_net if n < 0)
        if negative_months >= 2:
            add("alert", "critical", "Cash Flow Negativo",
                f"Cash flow BRL negativo em {negative_months} dos ultimos 3 meses. "
                f"Risco de liquidez.",
                f"{negative_months}/3 meses")

    # Sort: critical first, then warning, then info/positive
    sev_order = {"critical": 0, "warning": 1, "info": 2, "positive": 3}
    insights.sort(key=lambda x: sev_order.get(x["sev"], 9))

    return insights


# --- MAIN BUILD ---

def build():
    print("=" * 60)
    print("build_financial_data.py — CR FINANCIAL EZLINK")
    print("=" * 60)

    # ================================================================
    # 1. LOAD SERVICES
    # ================================================================
    print("\n[1/4] Loading Services CSVs...")
    created_files = sorted(
        glob.glob(os.path.join(SERVICES_CREATED_DIR, "CR_Services_Created_*.csv"))
    )
    legacy_files = sorted(
        glob.glob(os.path.join(SERVICES_DIR, "CR_Services_*.csv"))
    )
    svc_files = created_files + legacy_files
    print(f"  Found {len(created_files)} creation-indexed + {len(legacy_files)} legacy files")

    services = {}
    for fpath in svc_files:
        fname = os.path.basename(fpath)
        count = 0
        try:
            with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    sid = (row.get("id") or "").strip().strip('"')
                    if sid and sid not in services:
                        services[sid] = row
                        count += 1
        except Exception as e:
            print(f"  WARNING: {fname}: {e}")
    print(f"  Total unique services: {len(services)}")

    # ================================================================
    # 2. LOAD DEBITCREDIT
    # ================================================================
    print("\n[2/4] Loading DebitCredit CSVs...")
    dc_files = sorted(
        glob.glob(os.path.join(DEBITCREDIT_DIR, "CR_DebitCredit_*.csv"))
    )
    print(f"  Found {len(dc_files)} files")

    dc_rows = []
    dc_seen = set()
    for fpath in dc_files:
        fname = os.path.basename(fpath)
        count = 0
        try:
            with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f, delimiter=",")
                for row in reader:
                    dcid = (row.get("id") or "").strip().strip('"')
                    if dcid and dcid not in dc_seen:
                        dc_seen.add(dcid)
                        dc_rows.append(row)
                        count += 1
        except Exception as e:
            print(f"  WARNING: {fname}: {e}")
    print(f"  Total unique DC rows: {len(dc_rows)}")

    # ================================================================
    # 3. LOAD TICKETS
    # ================================================================
    print("\n[3/4] Loading Tickets CSVs...")
    tkt_files = sorted(
        glob.glob(os.path.join(TICKETS_DIR, "CR_Tickets_*.csv"))
    )
    print(f"  Found {len(tkt_files)} files")

    tickets = {}
    for fpath in tkt_files:
        fname = os.path.basename(fpath)
        count = 0
        try:
            with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    tid = (row.get("id") or "").strip().strip('"')
                    if tid and tid not in tickets:
                        tickets[tid] = row
                        count += 1
        except Exception as e:
            print(f"  WARNING: {fname}: {e}")
    print(f"  Total unique tickets: {len(tickets)}")

    # ================================================================
    # 4. AGGREGATE FINANCIAL DATA
    # ================================================================
    print("\n[4/4] Aggregating financial data...")

    # --- Accumulators ---

    # Cash Flow monthly (by service_date for services, by finan_date for DC)
    cf_monthly = defaultdict(lambda: {
        "inflows_brl": 0.0, "outflows_brl": 0.0, "net_brl": 0.0,
        "inflows_usd": 0.0, "outflows_usd": 0.0, "net_usd": 0.0,
        "dc_credits": 0.0, "dc_debits": 0.0, "dc_net": 0.0,
    })
    # Cash Flow daily (from DC)
    cf_daily_data = defaultdict(lambda: [0.0, 0.0])  # date -> [credits, debits]

    # AR by agency
    ar_agency = defaultdict(lambda: {
        "total_sale": 0.0, "invoiced": 0.0, "not_invoiced": 0.0,
        "n_services": 0, "n_invoiced": 0,
    })
    # AR aging buckets
    ar_aging = {"current": [0.0, 0], "30d": [0.0, 0], "60d": [0.0, 0],
                "90d": [0.0, 0], "120d+": [0.0, 0]}
    # AR aging by agency
    ar_aging_ag = defaultdict(lambda: {"current": 0.0, "30d": 0.0, "60d": 0.0,
                                        "90d": 0.0, "120d+": 0.0})

    # AP by provider
    ap_provider = defaultdict(lambda: {
        "total_cost": 0.0, "n_services": 0,
        "currencies": defaultdict(float),
    })
    # AP monthly
    ap_monthly = defaultdict(lambda: defaultdict(lambda: {"cost": 0.0, "n": 0}))

    # Revenue Recognition (3 timelines)
    rev_svc_date = defaultdict(lambda: {"gross": 0.0, "cost": 0.0, "profit": 0.0, "n": 0})
    rev_created = defaultdict(lambda: {"gross": 0.0, "cost": 0.0, "profit": 0.0, "n": 0})
    rev_finan = defaultdict(lambda: {"gross": 0.0, "credits": 0.0, "debits": 0.0, "n": 0})

    # P&L monthly
    pnl_monthly = defaultdict(lambda: {
        "revenue_gross": 0.0, "cost_provider": 0.0, "gross_profit": 0.0,
        "commissions": 0.0, "refunds_client": 0.0, "refunds_provider": 0.0,
        "cancellation_cost": 0.0, "net_operating": 0.0,
        "n_total": 0, "n_conf": 0, "n_canc": 0,
    })
    # P&L by agency (monthly)
    pnl_agency = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0, "comm": 0.0,
        "ref_c": 0.0, "ref_p": 0.0, "canc_cost": 0.0, "n": 0,
    }))
    # P&L by provider (monthly)
    pnl_provider = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0, "n": 0,
    }))

    # Currency monthly
    curr_monthly = defaultdict(lambda: defaultdict(lambda: {
        "sale": 0.0, "cost": 0.0, "profit": 0.0, "n": 0,
        "rate_sum": 0.0, "rate_n": 0,
    }))

    # Payment mix monthly
    pay_monthly = defaultdict(lambda: defaultdict(lambda: {"n": 0, "amount": 0.0}))
    # Source mix monthly
    src_monthly = defaultdict(lambda: defaultdict(lambda: {"n": 0, "amount": 0.0}))

    # Commission by agency (all-time)
    comm_agency = defaultdict(lambda: {"total": 0.0, "n": 0})
    # Commission monthly
    comm_monthly = defaultdict(lambda: {"total": 0.0, "n": 0})

    # Refund monthly
    ref_monthly = defaultdict(lambda: {
        "ref_client": 0.0, "ref_provider": 0.0, "n": 0,
    })
    # Refund by agency
    ref_agency = defaultdict(lambda: {"ref_c": 0.0, "ref_p": 0.0, "n": 0})
    # Refund by provider
    ref_provider = defaultdict(lambda: {"ref_c": 0.0, "ref_p": 0.0, "n": 0})

    # Disputes
    disp_monthly = defaultdict(lambda: {"n": 0})
    disp_agency = defaultdict(int)
    disp_hotel = defaultdict(int)

    # Invoice/NF from DC
    inv_monthly = defaultdict(lambda: {
        "invoiced_n": 0, "not_invoiced_n": 0,
        "invoiced_amt": 0.0, "not_invoiced_amt": 0.0,
        "nf_emitted_n": 0, "nf_pending_n": 0,
    })
    nf_agency = defaultdict(lambda: {"with_nf": 0, "without_nf": 0})

    # DSO by agency
    dso_agency = defaultdict(lambda: [0.0, 0])  # [sum_days, n]
    dso_monthly = defaultdict(lambda: [0.0, 0])

    # Margin accumulators (confirmed only, all-time)
    margin_agency = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "mkp_sum": 0.0, "mkp_n": 0, "n": 0,
    })
    margin_provider = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "mkp_sum": 0.0, "mkp_n": 0, "n": 0,
    })
    margin_hotel = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "mkp_sum": 0.0, "mkp_n": 0, "n": 0,
    })
    margin_monthly = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "mkp_sum": 0.0, "mkp_n": 0, "n": 0,
    })

    # Negative margin services (confirmed with loss)
    neg_margin = []

    # Cancellation monthly
    canc_monthly = defaultdict(lambda: {
        "n": 0, "lost_rev": 0.0, "penalties": 0.0, "refunds_paid": 0.0,
    })
    canc_agency = defaultdict(lambda: {"n": 0, "lost_rev": 0.0, "penalties": 0.0})

    # Upcoming check-ins (future confirmed)
    upcoming = defaultdict(lambda: {"value": 0.0, "n": 0})

    # Daily data for current year (2026+)
    CURRENT_YEAR = str(date.today().year)
    # daily by service_date: date -> {rev, cost, profit, n_conf, n_canc, n_total, refunds, comm}
    daily_svc = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "n_conf": 0, "n_canc": 0, "n_total": 0,
        "refunds": 0.0, "comm": 0.0,
    })
    # daily by created_at: same structure
    daily_created = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "n_conf": 0, "n_canc": 0, "n_total": 0,
        "refunds": 0.0, "comm": 0.0,
    })

    today_dt = datetime.now()

    # ================================================================
    # PROCESS SERVICES
    # ================================================================
    print("  Processing services...")
    svc_count = 0
    for sid, row in services.items():
        agency = (row.get("agency_name") or "").strip().strip('"')
        if agency == "TRAVELEZ":
            continue

        status = (row.get("status") or "").strip().strip('"')
        sc = status_code(status)
        hotel = (row.get("hotel_name") or "").strip().strip('"')
        provider = (row.get("provider_name") or "").strip().strip('"')
        currency = (row.get("currency") or "").strip().strip('"')

        price_sale = parse_float(row.get("price_sale", "0"))
        price_provider = parse_float(row.get("price_provider", "0"))
        markup_val = parse_float(row.get("markup", "0"))
        exchange_rate = parse_float(row.get("exchange_provider_finances", "0"))
        commission = parse_float(row.get("commission", "0"))
        refund_client = parse_float(row.get("refund_client", "0"))
        refund_provider = parse_float(row.get("refund_provider", "0"))
        canc_price = parse_float(row.get("cancellation_price", "0"))
        invoiced = parse_int(row.get("invoiced", "0"))
        payment_type = (row.get("payment_type") or "").strip().strip('"')
        source = (row.get("source") or "").strip().strip('"')
        dispute = (row.get("dispute") or row.get("dispute_v2") or "").strip().strip('"')

        service_date = parse_date(row.get("service_date", ""))
        created_at = parse_date(row.get("created_at", ""))

        # BRL conversion
        if exchange_rate > 1:
            sale_brl = price_sale * exchange_rate
            cost_brl = price_provider * exchange_rate
        else:
            sale_brl = price_sale
            cost_brl = price_provider

        profit = cost_brl * markup_val / 100.0

        sd_mo = month_key(service_date)
        ca_mo = month_key(created_at)

        if not agency:
            continue

        svc_count += 1

        # -- Revenue Recognition --
        if sd_mo:
            r = rev_svc_date[sd_mo]
            r["gross"] += sale_brl
            r["cost"] += cost_brl
            r["profit"] += profit
            r["n"] += 1
        if ca_mo:
            r = rev_created[ca_mo]
            r["gross"] += sale_brl
            r["cost"] += cost_brl
            r["profit"] += profit
            r["n"] += 1

        # -- P&L Monthly (by service_date) --
        if sd_mo:
            p = pnl_monthly[sd_mo]
            p["n_total"] += 1
            if sc == "C":
                p["revenue_gross"] += sale_brl
                p["cost_provider"] += cost_brl
                p["gross_profit"] += profit
                p["commissions"] += commission
                p["n_conf"] += 1
            elif sc == "X":
                p["cancellation_cost"] += sale_brl
                p["n_canc"] += 1
            p["refunds_client"] += refund_client
            p["refunds_provider"] += refund_provider

            # P&L by agency
            pa = pnl_agency[sd_mo][agency]
            pa["n"] += 1
            if sc == "C":
                pa["rev"] += sale_brl
                pa["cost"] += cost_brl
                pa["profit"] += profit
                pa["comm"] += commission
            pa["ref_c"] += refund_client
            pa["ref_p"] += refund_provider
            if sc == "X":
                pa["canc_cost"] += sale_brl

            # P&L by provider
            if provider:
                pp = pnl_provider[sd_mo][provider]
                pp["n"] += 1
                if sc == "C":
                    pp["rev"] += sale_brl
                    pp["cost"] += cost_brl
                    pp["profit"] += profit

        # -- Cash Flow (confirmed = inflow) --
        if sd_mo and sc == "C":
            cf = cf_monthly[sd_mo]
            if currency and currency.upper() == "USD":
                cf["inflows_usd"] += price_sale
                cf["outflows_usd"] += price_provider
            else:
                cf["inflows_brl"] += sale_brl
                cf["outflows_brl"] += cost_brl

        # -- AR by agency (confirmed services) --
        if sc == "C" and agency:
            ar = ar_agency[agency]
            ar["total_sale"] += sale_brl
            ar["n_services"] += 1
            if invoiced == 1:
                ar["invoiced"] += sale_brl
                ar["n_invoiced"] += 1
            else:
                ar["not_invoiced"] += sale_brl

            # AR aging (confirmed, not invoiced, by service_date)
            if invoiced == 0 and service_date:
                age_days = (today_dt - service_date).days
                if age_days < 0:
                    age_days = 0
                if age_days <= 30:
                    bk = "current"
                elif age_days <= 60:
                    bk = "30d"
                elif age_days <= 90:
                    bk = "60d"
                elif age_days <= 120:
                    bk = "90d"
                else:
                    bk = "120d+"
                ar_aging[bk][0] += sale_brl
                ar_aging[bk][1] += 1
                ar_aging_ag[agency][bk] += sale_brl

        # -- AP by provider --
        if sc == "C" and provider:
            ap = ap_provider[provider]
            ap["total_cost"] += cost_brl
            ap["n_services"] += 1
            ap["currencies"][currency or "BRL"] += cost_brl
            if sd_mo:
                apm = ap_monthly[sd_mo][provider]
                apm["cost"] += cost_brl
                apm["n"] += 1

        # -- Currency monthly --
        if sd_mo and sc == "C":
            cur = currency or "BRL"
            cm = curr_monthly[sd_mo][cur]
            cm["sale"] += sale_brl
            cm["cost"] += cost_brl
            cm["profit"] += profit
            cm["n"] += 1
            if exchange_rate > 1:
                cm["rate_sum"] += exchange_rate
                cm["rate_n"] += 1

        # -- Payment mix --
        if sd_mo and payment_type and sc == "C":
            pm = pay_monthly[sd_mo][payment_type]
            pm["n"] += 1
            pm["amount"] += sale_brl
        if sd_mo and source and sc == "C":
            sm = src_monthly[sd_mo][source]
            sm["n"] += 1
            sm["amount"] += sale_brl

        # -- Commission --
        if commission > 0 and sc == "C":
            comm_agency[agency]["total"] += commission
            comm_agency[agency]["n"] += 1
            if sd_mo:
                comm_monthly[sd_mo]["total"] += commission
                comm_monthly[sd_mo]["n"] += 1

        # -- Refunds --
        if refund_client > 0 or refund_provider > 0:
            if sd_mo:
                rm = ref_monthly[sd_mo]
                rm["ref_client"] += refund_client
                rm["ref_provider"] += refund_provider
                rm["n"] += 1
            ref_agency[agency]["ref_c"] += refund_client
            ref_agency[agency]["ref_p"] += refund_provider
            ref_agency[agency]["n"] += 1
            if provider:
                ref_provider[provider]["ref_c"] += refund_client
                ref_provider[provider]["ref_p"] += refund_provider
                ref_provider[provider]["n"] += 1

        # -- Disputes --
        if dispute and dispute not in ("", "0", "false", "False"):
            if sd_mo:
                disp_monthly[sd_mo]["n"] += 1
            if agency:
                disp_agency[agency] += 1
            if hotel:
                disp_hotel[hotel] += 1

        # -- Margin (confirmed only) --
        if sc == "C":
            for acc, key in [(margin_agency, agency), (margin_provider, provider), (margin_hotel, hotel)]:
                if key:
                    m = acc[key]
                    m["rev"] += sale_brl
                    m["cost"] += cost_brl
                    m["profit"] += profit
                    m["n"] += 1
                    if markup_val != 0:
                        m["mkp_sum"] += markup_val
                        m["mkp_n"] += 1
            if sd_mo:
                mm = margin_monthly[sd_mo]
                mm["rev"] += sale_brl
                mm["cost"] += cost_brl
                mm["profit"] += profit
                mm["n"] += 1
                if markup_val != 0:
                    mm["mkp_sum"] += markup_val
                    mm["mkp_n"] += 1

            # Negative margin detection
            if profit < -1 and len(neg_margin) < 500:
                neg_margin.append({
                    "id": sid, "agency": agency, "hotel": hotel,
                    "provider": provider, "rev": r2(sale_brl),
                    "cost": r2(cost_brl), "loss": r2(profit),
                    "date": date_key(service_date) or "",
                })

        # -- Cancellation --
        if sc == "X" and sd_mo:
            cm = canc_monthly[sd_mo]
            cm["n"] += 1
            cm["lost_rev"] += sale_brl
            cm["penalties"] += canc_price
            cm["refunds_paid"] += refund_client
            ca = canc_agency[agency]
            ca["n"] += 1
            ca["lost_rev"] += sale_brl
            ca["penalties"] += canc_price

        # -- Upcoming check-ins (future confirmed) --
        if sc == "C" and service_date and service_date > today_dt:
            umo = month_key(service_date)
            if umo:
                upcoming[umo]["value"] += sale_brl
                upcoming[umo]["n"] += 1

        # -- Daily data for current year --
        sd_day = date_key(service_date)
        ca_day = date_key(created_at)
        if sd_day and sd_day.startswith(CURRENT_YEAR):
            d = daily_svc[sd_day]
            d["n_total"] += 1
            if sc == "C":
                d["rev"] += sale_brl
                d["cost"] += cost_brl
                d["profit"] += profit
                d["n_conf"] += 1
                d["comm"] += commission
            elif sc == "X":
                d["n_canc"] += 1
            d["refunds"] += refund_client
        if ca_day and ca_day.startswith(CURRENT_YEAR):
            d = daily_created[ca_day]
            d["n_total"] += 1
            if sc == "C":
                d["rev"] += sale_brl
                d["cost"] += cost_brl
                d["profit"] += profit
                d["n_conf"] += 1
                d["comm"] += commission
            elif sc == "X":
                d["n_canc"] += 1
            d["refunds"] += refund_client

    print(f"  Processed {svc_count} services")

    # ================================================================
    # PROCESS DEBITCREDIT
    # ================================================================
    print("  Processing DebitCredit...")
    dc_count = 0
    for row in dc_rows:
        finan_date = parse_date(row.get("finan_date", ""))
        finan_type = parse_int(row.get("finan_type", "0"))
        finan_net = parse_float(row.get("finan_net", "0"))
        finan_net_cur = (row.get("finan_net_cur") or "BRL").strip().strip('"')
        finan_sale = parse_float(row.get("finan_sale", "0"))
        finan_status = parse_int(row.get("finan_status", "0"))
        agency = (row.get("agency_name") or "").strip().strip('"')
        provider = (row.get("provider_name") or "").strip().strip('"')
        invoiced_val = parse_int(row.get("invoiced", "0"))
        nf_val = parse_int(row.get("nf", "0"))
        price_sale = parse_float(row.get("price_sale", "0"))
        reservation_date = parse_date(row.get("reservation_date", ""))

        fd_mo = month_key(finan_date)
        fd_day = date_key(finan_date)

        if not fd_mo:
            continue

        dc_count += 1

        # Cash Flow from DC
        cf = cf_monthly[fd_mo]
        if finan_type == 1:  # credit
            cf["dc_credits"] += finan_net
        elif finan_type == 2:  # debit
            cf["dc_debits"] += finan_net

        # Daily cash flow
        if fd_day:
            d = cf_daily_data[fd_day]
            if finan_type == 1:
                d[0] += finan_net
            elif finan_type == 2:
                d[1] += finan_net

        # Revenue by finan_date
        r = rev_finan[fd_mo]
        r["gross"] += price_sale
        if finan_type == 1:
            r["credits"] += finan_net
        elif finan_type == 2:
            r["debits"] += finan_net
        r["n"] += 1

        # Invoice/NF status
        iv = inv_monthly[fd_mo]
        if invoiced_val == 1:
            iv["invoiced_n"] += 1
            iv["invoiced_amt"] += price_sale
        else:
            iv["not_invoiced_n"] += 1
            iv["not_invoiced_amt"] += price_sale
        if nf_val == 1:
            iv["nf_emitted_n"] += 1
        else:
            iv["nf_pending_n"] += 1

        # NF by agency
        if agency:
            na = nf_agency[agency]
            if nf_val == 1:
                na["with_nf"] += 1
            else:
                na["without_nf"] += 1

        # DSO calculation
        if finan_type == 1 and finan_date and reservation_date and agency:
            dso_days = (finan_date - reservation_date).days
            if 0 <= dso_days <= 365:
                dso_agency[agency][0] += dso_days
                dso_agency[agency][1] += 1
                dso_monthly[fd_mo][0] += dso_days
                dso_monthly[fd_mo][1] += 1

    print(f"  Processed {dc_count} DC rows")

    # ================================================================
    # PROCESS TICKETS (financial aspects only)
    # ================================================================
    print("  Processing Tickets (financial)...")
    tkt_financial_count = 0
    tkt_monthly_count = defaultdict(int)
    for tid, row in tickets.items():
        created = parse_date(row.get("created_at", ""))
        tmo = month_key(created)
        if tmo:
            tkt_monthly_count[tmo] += 1
        charges = parse_float(row.get("charges", "0"))
        if charges > 0:
            tkt_financial_count += 1
    print(f"  Tickets with charges: {tkt_financial_count}")

    # ================================================================
    # FINALIZE P&L
    # ================================================================
    for mo, p in pnl_monthly.items():
        p["net_operating"] = (
            p["gross_profit"]
            - p["commissions"]
            - p["refunds_client"]
        )

    # Finalize cash flow net
    for mo, cf in cf_monthly.items():
        cf["net_brl"] = cf["inflows_brl"] - cf["outflows_brl"]
        cf["net_usd"] = cf["inflows_usd"] - cf["outflows_usd"]
        cf["dc_net"] = cf["dc_credits"] - cf["dc_debits"]

    # ================================================================
    # BUILD OUTPUT JSON
    # ================================================================
    print("\n  Building JSON output...")

    # Sort negative margin by loss
    neg_margin.sort(key=lambda x: x["loss"])

    # Build cashflow_daily sorted
    cf_daily_sorted = sorted(cf_daily_data.keys())
    cf_daily_out = {
        "d": cf_daily_sorted,
        "credits": [r2(cf_daily_data[d][0]) for d in cf_daily_sorted],
        "debits": [r2(cf_daily_data[d][1]) for d in cf_daily_sorted],
        "net": [r2(cf_daily_data[d][0] - cf_daily_data[d][1]) for d in cf_daily_sorted],
    }

    # Build AR aging output
    ar_aging_out = {}
    for bk in ["current", "30d", "60d", "90d", "120d+"]:
        ar_aging_out[bk] = {"amount": r2(ar_aging[bk][0]), "count": ar_aging[bk][1]}

    # AR aging by agency (top 50 by total not invoiced)
    ar_aging_ag_out = {}
    top_ar_agencies = sorted(ar_aging_ag.keys(),
                             key=lambda a: sum(ar_aging_ag[a].values()), reverse=True)[:50]
    for ag in top_ar_agencies:
        ar_aging_ag_out[ag] = {k: r2(v) for k, v in ar_aging_ag[ag].items()}

    # AP provider output (top 50)
    ap_out = {}
    top_ap = sorted(ap_provider.keys(), key=lambda p: ap_provider[p]["total_cost"], reverse=True)[:50]
    for p in top_ap:
        d = ap_provider[p]
        ap_out[p] = {
            "total_cost": r2(d["total_cost"]),
            "n_services": d["n_services"],
            "currencies": {k: r2(v) for k, v in d["currencies"].items()},
        }

    # AP monthly (top 20 providers)
    ap_monthly_out = {}
    for mo in sorted(ap_monthly.keys()):
        ap_monthly_out[mo] = {}
        for p in list(sorted(ap_monthly[mo].keys(),
                              key=lambda x: ap_monthly[mo][x]["cost"], reverse=True))[:20]:
            d = ap_monthly[mo][p]
            ap_monthly_out[mo][p] = {"cost": r2(d["cost"]), "n": d["n"]}

    # Margin outputs
    def margin_out(acc, top_n=100):
        result = {}
        top = sorted(acc.keys(), key=lambda k: acc[k]["rev"], reverse=True)[:top_n]
        for k in top:
            m = acc[k]
            rev = m["rev"]
            result[k] = {
                "rev": r2(rev), "cost": r2(m["cost"]), "profit": r2(m["profit"]),
                "margin": r2(m["profit"] / rev * 100) if rev > 0 else 0,
                "mkp_avg": r2(m["mkp_sum"] / m["mkp_n"]) if m["mkp_n"] > 0 else 0,
                "n": m["n"],
            }
        return result

    # Margin monthly trend
    margin_monthly_out = {}
    for mo in sorted(margin_monthly.keys()):
        m = margin_monthly[mo]
        rev = m["rev"]
        margin_monthly_out[mo] = {
            "margin_pct": r2(m["profit"] / rev * 100) if rev > 0 else 0,
            "mkp_avg": r2(m["mkp_sum"] / m["mkp_n"]) if m["mkp_n"] > 0 else 0,
            "rev": r2(rev), "profit": r2(m["profit"]),
        }

    # DSO outputs
    dso_ag_out = {}
    for ag in sorted(dso_agency.keys()):
        s, n = dso_agency[ag]
        if n > 0:
            dso_ag_out[ag] = {"avg_dso": r2(s / n), "n": n}
    dso_monthly_out = {}
    for mo in sorted(dso_monthly.keys()):
        s, n = dso_monthly[mo]
        if n > 0:
            dso_monthly_out[mo] = {"avg_dso": r2(s / n), "n": n}

    # Currency monthly output
    curr_monthly_out = {}
    for mo in sorted(curr_monthly.keys()):
        curr_monthly_out[mo] = {}
        for cur, d in curr_monthly[mo].items():
            curr_monthly_out[mo][cur] = {
                "sale": r2(d["sale"]), "cost": r2(d["cost"]),
                "profit": r2(d["profit"]), "n": d["n"],
                "avg_rate": r2(d["rate_sum"] / d["rate_n"]) if d["rate_n"] > 0 else 0,
            }

    # Invoice summary
    total_inv = sum(v["invoiced_n"] for v in inv_monthly.values())
    total_not_inv = sum(v["not_invoiced_n"] for v in inv_monthly.values())
    total_nf = sum(v["nf_emitted_n"] for v in inv_monthly.values())
    total_nf_pend = sum(v["nf_pending_n"] for v in inv_monthly.values())
    total_inv_amt = sum(v["invoiced_amt"] for v in inv_monthly.values())
    total_not_inv_amt = sum(v["not_invoiced_amt"] for v in inv_monthly.values())

    invoice_summary = {
        "total_invoiced": total_inv,
        "total_not_invoiced": total_not_inv,
        "total_nf_emitted": total_nf,
        "total_nf_pending": total_nf_pend,
        "invoiced_amt": r2(total_inv_amt),
        "not_invoiced_amt": r2(total_not_inv_amt),
        "invoiced_pct": r2(total_inv / (total_inv + total_not_inv) * 100) if (total_inv + total_not_inv) > 0 else 0,
        "nf_pct": r2(total_nf / (total_nf + total_nf_pend) * 100) if (total_nf + total_nf_pend) > 0 else 0,
    }

    # NF by agency (top 50)
    nf_ag_out = {}
    top_nf_ag = sorted(nf_agency.keys(),
                       key=lambda a: nf_agency[a]["with_nf"] + nf_agency[a]["without_nf"],
                       reverse=True)[:50]
    for ag in top_nf_ag:
        d = nf_agency[ag]
        total = d["with_nf"] + d["without_nf"]
        nf_ag_out[ag] = {
            "with_nf": d["with_nf"], "without_nf": d["without_nf"],
            "pct": r2(d["with_nf"] / total * 100) if total > 0 else 0,
        }

    # Round all monetary values in dicts
    def round_dict(d):
        out = {}
        for k, v in d.items():
            if isinstance(v, float):
                out[k] = r2(v)
            elif isinstance(v, dict):
                out[k] = round_dict(v)
            else:
                out[k] = v
        return out

    # Assemble output
    output = {
        "metadata": {
            "generated": TODAY,
            "total_services": len(services),
            "total_dc_rows": len(dc_rows),
            "total_tickets": len(tickets),
        },

        # Cash Flow
        "cashflow_monthly": {mo: round_dict(cf_monthly[mo]) for mo in sorted(cf_monthly.keys())},
        "cashflow_daily": cf_daily_out,

        # AR / AP
        "ar_by_agency": {ag: round_dict(ar_agency[ag])
                         for ag in sorted(ar_agency.keys(),
                                          key=lambda a: ar_agency[a]["not_invoiced"],
                                          reverse=True)[:50]},
        "ar_aging": ar_aging_out,
        "ar_aging_by_agency": ar_aging_ag_out,
        "ap_by_provider": ap_out,
        "ap_monthly": ap_monthly_out,

        # Revenue Recognition
        "rev_by_service_date": {mo: round_dict(rev_svc_date[mo]) for mo in sorted(rev_svc_date.keys())},
        "rev_by_created_date": {mo: round_dict(rev_created[mo]) for mo in sorted(rev_created.keys())},
        "rev_by_finan_date": {mo: round_dict(rev_finan[mo]) for mo in sorted(rev_finan.keys())},

        # P&L
        "pnl_monthly": {mo: round_dict(pnl_monthly[mo]) for mo in sorted(pnl_monthly.keys())},
        "pnl_by_agency": {mo: {ag: round_dict(v) for ag, v in
                                sorted(data.items(), key=lambda x: x[1]["rev"], reverse=True)[:30]}
                          for mo, data in sorted(pnl_agency.items())},
        "pnl_by_provider": {mo: {p: round_dict(v) for p, v in
                                  sorted(data.items(), key=lambda x: x[1]["rev"], reverse=True)[:20]}
                            for mo, data in sorted(pnl_provider.items())},

        # Margin
        "margin_by_agency": margin_out(margin_agency, 100),
        "margin_by_provider": margin_out(margin_provider, 50),
        "margin_by_hotel_top100": margin_out(margin_hotel, 100),
        "margin_monthly_trend": margin_monthly_out,
        "negative_margin_services": neg_margin[:200],

        # Currency
        "currency_monthly": curr_monthly_out,

        # Payment & Source
        "payment_mix_monthly": {mo: {pt: round_dict(v) for pt, v in d.items()}
                                for mo, d in sorted(pay_monthly.items())},
        "source_mix_monthly": {mo: {s: round_dict(v) for s, v in d.items()}
                               for mo, d in sorted(src_monthly.items())},

        # Commissions
        "commission_by_agency": {ag: round_dict(comm_agency[ag])
                                 for ag in sorted(comm_agency.keys(),
                                                  key=lambda a: comm_agency[a]["total"],
                                                  reverse=True)[:50]},
        "commission_monthly": {mo: round_dict(comm_monthly[mo]) for mo in sorted(comm_monthly.keys())},

        # Refunds
        "refund_monthly": {mo: round_dict(ref_monthly[mo]) for mo in sorted(ref_monthly.keys())},
        "refund_by_agency": {ag: round_dict(ref_agency[ag])
                             for ag in sorted(ref_agency.keys(),
                                              key=lambda a: ref_agency[a]["ref_c"],
                                              reverse=True)[:50]},
        "refund_by_provider": {p: round_dict(ref_provider[p])
                               for p in sorted(ref_provider.keys(),
                                               key=lambda x: ref_provider[x]["ref_c"],
                                               reverse=True)[:30]},

        # Disputes
        "disputes_monthly": {mo: disp_monthly[mo] for mo in sorted(disp_monthly.keys())},
        "disputes_by_agency": dict(sorted(disp_agency.items(), key=lambda x: x[1], reverse=True)[:30]),
        "disputes_by_hotel": dict(sorted(disp_hotel.items(), key=lambda x: x[1], reverse=True)[:30]),

        # Invoice / NF
        "invoice_summary": invoice_summary,
        "invoice_monthly": {mo: round_dict(inv_monthly[mo]) for mo in sorted(inv_monthly.keys())},
        "nf_by_agency": nf_ag_out,

        # DSO
        "dso_by_agency": dso_ag_out,
        "dso_monthly": dso_monthly_out,

        # Cancellation
        "cancellation_monthly": {mo: round_dict(canc_monthly[mo]) for mo in sorted(canc_monthly.keys())},
        "cancellation_by_agency": {ag: round_dict(canc_agency[ag])
                                   for ag in sorted(canc_agency.keys(),
                                                    key=lambda a: canc_agency[a]["lost_rev"],
                                                    reverse=True)[:50]},

        # Upcoming
        "upcoming_checkins": {mo: round_dict(upcoming[mo]) for mo in sorted(upcoming.keys())[:6]},

        # Tickets monthly count
        "tickets_monthly": dict(sorted(tkt_monthly_count.items())),

        # Daily data current year (by service_date)
        "daily_by_service_date": {d: round_dict(daily_svc[d])
                                  for d in sorted(daily_svc.keys())},
        # Daily data current year (by created_at)
        "daily_by_created_date": {d: round_dict(daily_created[d])
                                   for d in sorted(daily_created.keys())},
        "current_year": CURRENT_YEAR,
    }

    # ================================================================
    # AUTO-GENERATIVE AI INSIGHTS
    # ================================================================
    print("\n  Generating AI Insights...")
    insights = generate_insights(output)
    output["ai_insights"] = insights
    print(f"  Generated {len(insights)} insights")

    # ================================================================
    # INJECT INTO HTML
    # ================================================================
    print("\n  Injecting into Dashboard_Financeiro.html...")

    json_str = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    print(f"  JSON size: {len(json_str):,} bytes ({len(json_str)/1024/1024:.1f} MB)")

    if not os.path.exists(HTML_FILE):
        print(f"  WARNING: {HTML_FILE} not found — writing JSON to dashboard_data.json instead")
        with open(os.path.join(BASE_DIR, "dashboard_data.json"), "w", encoding="utf-8") as f:
            f.write(json_str)
        print("  Done! Run again after creating Dashboard_Financeiro.html")
        return output

    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    marker_start = '<script id="dashData" type="application/json">'
    marker_end = "</script>"
    idx_start = html.find(marker_start)
    if idx_start == -1:
        print("  ERROR: marker <script id='dashData'> not found in HTML!")
        with open(os.path.join(BASE_DIR, "dashboard_data.json"), "w", encoding="utf-8") as f:
            f.write(json_str)
        return output

    idx_content_start = idx_start + len(marker_start)
    idx_end = html.find(marker_end, idx_content_start)

    new_html = html[:idx_content_start] + "\n" + json_str + "\n" + html[idx_end:]

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(new_html)

    print(f"  Dashboard updated: {HTML_FILE}")
    print("=" * 60)
    print("DONE!")
    return output


if __name__ == "__main__":
    build()
