#!/usr/bin/env python3
"""
build_financial_data.py — CR FINANCIAL EZLINK Dashboard Builder (v3)

Reads Services, Services_Created, DebitCredit, Tickets CSVs and aggregates
comprehensive financial metrics for the Financial Dashboard.

Produces JSON with 60+ aggregation keys covering:
  P&L, Cash Flow, Revenue Recognition, AR/AP, Margin, Cancellation,
  Currency, Payment, Source, Commissions, Refunds, Disputes, Invoice/NF/DSO,
  YoY Comparison, Geographic, Room Nights, Lead Time, Integration/Channel,
  Agency Ranking, Daily/Weekly Rollups, Tickets Deep, Protection/Refundable,
  Booking Value Distribution, Payment Deadline Compliance, AI Insights (40+).

Usage:
    python3 build_financial_data.py
"""

import csv
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta

# ============================================================================
# CONFIG
# ============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_FILE = os.path.join(BASE_DIR, "Dashboard_Financeiro.html")
SERVICES_DIR = os.path.join(BASE_DIR, "Services")
SERVICES_CREATED_DIR = os.path.join(BASE_DIR, "Services_Created")
TICKETS_DIR = os.path.join(BASE_DIR, "Tickets")
DEBITCREDIT_DIR = os.path.join(BASE_DIR, "DebitCredit")

TODAY = date.today().isoformat()
CURRENT_YEAR = str(date.today().year)

# Filtro: somente dados de 2025 e 2026
MIN_YEAR = 2025
MAX_YEAR = 2026

# Top-N limits
TOP_AGENCIES = 100
TOP_PROVIDERS = 50
TOP_HOTELS = 100
TOP_CITIES = 50


# ============================================================================
# HELPERS
# ============================================================================

def parse_date(s):
    """Parse a date string flexibly (ISO formats, with/without T/Z/microseconds)."""
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
    """Safely parse a float, returning 0.0 on failure."""
    if not s or not isinstance(s, str):
        return 0.0
    s = s.strip().strip('"')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def parse_int(s):
    """Safely parse an int, returning 0 on failure."""
    if not s or not isinstance(s, str):
        return 0
    s = s.strip().strip('"')
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def month_key(dt):
    """Return YYYY-MM string from a datetime object."""
    if dt is None:
        return None
    return dt.strftime("%Y-%m")


def date_key(dt):
    """Return YYYY-MM-DD string from a datetime object."""
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def status_code(status_str):
    """Classify status to C=confirmed, X=cancelled, R=rejected, I=in_progress."""
    sl = (status_str or "").lower()
    if "confirmed" in sl:
        return "C"
    elif "cancelled" in sl or "canceled" in sl:
        return "X"
    elif "rejected" in sl:
        return "R"
    elif "in progress" in sl or "in_progress" in sl:
        return "I"
    return "C"


def is_truthy(val):
    """Check if a CSV field value is truthy (not empty, not 0, not false)."""
    if not val or not isinstance(val, str):
        return False
    v = val.strip().strip('"').lower()
    return v not in ("", "0", "false", "no", "null", "none")


def r2(v):
    """Round float to 2 decimal places."""
    return round(v, 2)


def round_dict(d):
    """Recursively round all float values in a dict to 2 decimal places."""
    out = {}
    for k, v in d.items():
        if isinstance(v, float):
            out[k] = r2(v)
        elif isinstance(v, dict):
            out[k] = round_dict(v)
        elif isinstance(v, list):
            out[k] = [r2(x) if isinstance(x, float) else x for x in v]
        else:
            out[k] = v
    return out


def safe_div(a, b, default=0.0):
    """Safe division returning default if divisor is zero."""
    return a / b if b != 0 else default


def get_week_key(dt):
    """Return ISO week key like '2026-W15' and week start/end dates."""
    iso = dt.isocalendar()
    year = iso[0]
    week = iso[1]
    key = f"{year}-W{week:02d}"
    # Week start (Monday) and end (Sunday)
    week_start = dt - timedelta(days=dt.weekday())
    week_end = week_start + timedelta(days=6)
    return key, week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")


def bucket_value(val):
    """Return the booking value distribution bucket for a given BRL amount."""
    if val < 500:
        return 0
    elif val < 1000:
        return 1
    elif val < 2000:
        return 2
    elif val < 5000:
        return 3
    elif val < 10000:
        return 4
    elif val < 20000:
        return 5
    elif val < 50000:
        return 6
    else:
        return 7


BUCKET_LABELS = ["0-500", "500-1K", "1K-2K", "2K-5K", "5K-10K", "10K-20K", "20K-50K", "50K+"]


# ============================================================================
# AI INSIGHTS ENGINE (40+ rules)
# ============================================================================

def generate_insights(data):
    """
    Auto-generative financial intelligence engine.
    Analyzes all aggregated data and produces categorized insights.
    Categories: alert, trend, opportunity, summary, yoy, daily
    Severities: critical, warning, info, positive
    """
    insights = []

    def add(category, severity, title, detail, metric=None):
        insights.append({
            "cat": category,
            "sev": severity,
            "title": title,
            "detail": detail,
            "metric": metric or "",
        })

    pnl = data.get("pnl_monthly", {})
    months = sorted(pnl.keys())
    mt = data.get("margin_monthly_trend", {})
    canc = data.get("cancellation_monthly", {})
    canc_ag = data.get("cancellation_by_agency", {})
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
    yoy = data.get("yoy", {})
    geo_total = data.get("geo_country_total", {})
    rn_monthly = data.get("room_nights_monthly", {})
    lt_monthly = data.get("lead_time_monthly", {})
    integration_total = data.get("integration_total", {})
    daily_svc = data.get("daily_by_service_date", {})
    weekly = data.get("weekly_current_year", {})
    tkt_monthly = data.get("tickets_monthly", {})
    tkt_fees = data.get("tickets_fees_monthly", {})
    prot_monthly = data.get("protection_monthly", {})
    pay_deadline = data.get("payment_deadline_monthly", {})
    inv_monthly = data.get("invoice_monthly", {})
    nf_agency = data.get("nf_by_agency", {})
    dso_monthly = data.get("dso_monthly", {})
    comm_ag = data.get("commission_by_agency", {})
    bvd = data.get("booking_value_distribution", {})
    curr_year = data.get("current_year", CURRENT_YEAR)

    total_rev = 0.0
    total_prof = 0.0
    total_net = 0.0
    total_n_conf = 0
    total_n_canc = 0
    total_n = 0

    if months:
        total_rev = sum(pnl[m].get("revenue_gross", 0) for m in months)
        total_prof = sum(pnl[m].get("gross_profit", 0) for m in months)
        total_net = sum(pnl[m].get("net_operating", 0) for m in months)
        total_n_conf = sum(pnl[m].get("n_conf", 0) for m in months)
        total_n_canc = sum(pnl[m].get("n_canc", 0) for m in months)
        total_n = sum(pnl[m].get("n_total", 0) for m in months)
        overall_margin = safe_div(total_prof, total_rev) * 100

    # ---------------------------------------------------------------
    # SUMMARY (rules 1-5)
    # ---------------------------------------------------------------
    if months:
        # 1. Overall volume and revenue
        add("summary", "info", "Volume Total",
            f"R$ {total_rev:,.0f} em revenue bruto com {data['metadata']['total_services']:,} servicos unicos "
            f"({total_n_conf:,} confirmados, {total_n_canc:,} cancelados) ao longo de {len(months)} meses.",
            f"R$ {total_rev:,.0f}")

        # 2. Margin health
        sev_margin = "positive" if overall_margin > 4 else ("info" if overall_margin > 2 else "warning")
        add("summary", sev_margin, "Margem Geral",
            f"Margem bruta de {overall_margin:.1f}% sobre todo o periodo. "
            f"Profit bruto total: R$ {total_prof:,.0f}. Net operating: R$ {total_net:,.0f}.",
            f"{overall_margin:.1f}%")

        # 3. Pipeline / upcoming
        upcoming_n = sum(v.get("n", 0) for v in upcoming.values())
        upcoming_val = sum(v.get("value", 0) for v in upcoming.values())
        if upcoming_n > 0:
            add("summary", "info", "Pipeline Futuro",
                f"{upcoming_n:,} reservas confirmadas para os proximos meses, "
                f"totalizando R$ {upcoming_val:,.0f}.",
                f"{upcoming_n:,} reservas")

        # 4. Payment collection efficiency
        inv_pct = inv_s.get("invoiced_pct", 0)
        add("summary", "positive" if inv_pct > 85 else "warning",
            "Eficiencia de Cobranca",
            f"{inv_pct:.0f}% dos registros financeiros faturados. "
            f"R$ {inv_s.get('invoiced_amt', 0):,.0f} faturado vs R$ {inv_s.get('not_invoiced_amt', 0):,.0f} pendente.",
            f"{inv_pct:.0f}%")

        # 5. YoY growth summary
        curr_year_months = [m for m in months if m.startswith(curr_year)]
        prev_year = str(int(curr_year) - 1)
        prev_year_months = [m for m in months if m.startswith(prev_year)]
        if curr_year_months and prev_year_months:
            max_mo_curr = max(m[5:] for m in curr_year_months)
            comparable_prev = [m for m in prev_year_months if m[5:] <= max_mo_curr]
            if comparable_prev:
                rev_curr = sum(pnl[m].get("revenue_gross", 0) for m in curr_year_months)
                rev_prev = sum(pnl[m].get("revenue_gross", 0) for m in comparable_prev)
                if rev_prev > 0:
                    yoy_growth = (rev_curr - rev_prev) / rev_prev * 100
                    sev = "positive" if yoy_growth > 0 else "warning"
                    add("summary", sev, "Crescimento YoY",
                        f"Revenue {curr_year} ate agora: R$ {rev_curr:,.0f} vs R$ {rev_prev:,.0f} "
                        f"no mesmo periodo de {prev_year} ({yoy_growth:+.1f}%).",
                        f"{yoy_growth:+.1f}%")

    # ---------------------------------------------------------------
    # YOY ANALYSIS (rules 6-10)
    # ---------------------------------------------------------------
    if yoy and months:
        prev_year = str(int(curr_year) - 1)
        # 6-8. YoY revenue, margin, volume for recent months
        current_month = int(date.today().strftime("%m"))
        for mo_offset in range(1, 4):
            mo_num = current_month - mo_offset
            if mo_num <= 0:
                mo_num += 12
            mo_str = f"{mo_num:02d}"
            mo_data = yoy.get(mo_str, {})
            cy_data = mo_data.get(curr_year, {})
            py_data = mo_data.get(prev_year, {})

            if cy_data.get("rev", 0) > 0 and py_data.get("rev", 0) > 0:
                rev_chg = safe_div(cy_data["rev"] - py_data["rev"], py_data["rev"]) * 100
                sev = "positive" if rev_chg > 0 else "warning"
                import calendar
                mo_name = calendar.month_abbr[mo_num]
                add("yoy", sev, f"YoY Revenue {mo_name}",
                    f"{mo_name} {curr_year}: R$ {cy_data['rev']:,.0f} vs {prev_year}: R$ {py_data['rev']:,.0f} ({rev_chg:+.1f}%).",
                    f"{rev_chg:+.1f}%")

            if cy_data.get("margin_pct", 0) > 0 and py_data.get("margin_pct", 0) > 0:
                mg_chg = cy_data["margin_pct"] - py_data["margin_pct"]
                if abs(mg_chg) > 1:
                    sev = "positive" if mg_chg > 0 else "warning"
                    add("yoy", sev, f"YoY Margem {mo_name}",
                        f"Margem {mo_name}: {cy_data['margin_pct']:.1f}% ({curr_year}) vs {py_data['margin_pct']:.1f}% ({prev_year}).",
                        f"{mg_chg:+.1f}pp")

        # 9. YoY cancellation rate
        for mo_str in [f"{current_month - 1:02d}" if current_month > 1 else "12"]:
            mo_data = yoy.get(mo_str, {})
            cy_d = mo_data.get(curr_year, {})
            py_d = mo_data.get(prev_year, {})
            if cy_d.get("n_conf", 0) + cy_d.get("n_canc", 0) > 0 and py_d.get("n_conf", 0) + py_d.get("n_canc", 0) > 0:
                cr_cy = safe_div(cy_d["n_canc"], cy_d["n_conf"] + cy_d["n_canc"]) * 100
                cr_py = safe_div(py_d["n_canc"], py_d["n_conf"] + py_d["n_canc"]) * 100
                if abs(cr_cy - cr_py) > 2:
                    sev = "positive" if cr_cy < cr_py else "warning"
                    add("yoy", sev, "YoY Taxa de Cancelamento",
                        f"Taxa cancelamento: {cr_cy:.1f}% ({curr_year}) vs {cr_py:.1f}% ({prev_year}).",
                        f"{cr_cy - cr_py:+.1f}pp")

        # 10. Seasonal pattern detection
        if len(yoy) >= 6:
            month_totals = {}
            for mo_str, years_data in yoy.items():
                total_rev_mo = sum(yd.get("rev", 0) for yd in years_data.values())
                month_totals[mo_str] = total_rev_mo
            if month_totals:
                best_mo = max(month_totals, key=month_totals.get)
                worst_mo = min(month_totals, key=month_totals.get)
                if month_totals[best_mo] > 0:
                    import calendar
                    add("yoy", "info", "Sazonalidade Detectada",
                        f"Mes mais forte historicamente: {calendar.month_abbr[int(best_mo)]}. "
                        f"Mes mais fraco: {calendar.month_abbr[int(worst_mo)]}.",
                        f"Pico: {best_mo}")

    # ---------------------------------------------------------------
    # DAILY ANALYSIS (rules 11-15)
    # ---------------------------------------------------------------
    if daily_svc:
        sorted_days = sorted(daily_svc.keys())
        today_str = date.today().isoformat()
        recent_days = [d for d in sorted_days if d <= today_str]

        if len(recent_days) >= 7:
            last7 = recent_days[-7:]
            avg7_rev = sum(daily_svc[d].get("rev", 0) for d in last7) / 7
            avg7_n = sum(daily_svc[d].get("n_conf", 0) for d in last7) / 7

            # 11. Today vs 7-day average
            if today_str in daily_svc:
                today_d = daily_svc[today_str]
                today_rev = today_d.get("rev", 0)
                if avg7_rev > 0:
                    ratio = today_rev / avg7_rev
                    if ratio > 1.5:
                        add("daily", "positive", "Dia Acima da Media",
                            f"Revenue hoje: R$ {today_rev:,.0f} ({ratio:.1f}x a media 7d de R$ {avg7_rev:,.0f}).",
                            f"{ratio:.1f}x")
                    elif ratio < 0.5 and today_rev > 0:
                        add("daily", "warning", "Dia Abaixo da Media",
                            f"Revenue hoje: R$ {today_rev:,.0f} (apenas {ratio:.1%} da media 7d).",
                            f"{ratio:.1%}")

            # 12. WoW comparison
            if len(recent_days) >= 14:
                this_week = recent_days[-7:]
                last_week = recent_days[-14:-7]
                tw_rev = sum(daily_svc[d].get("rev", 0) for d in this_week)
                lw_rev = sum(daily_svc[d].get("rev", 0) for d in last_week)
                if lw_rev > 0:
                    wow = (tw_rev - lw_rev) / lw_rev * 100
                    sev = "positive" if wow > 0 else "warning"
                    add("daily", sev, "Semana vs Semana Anterior",
                        f"Revenue esta semana: R$ {tw_rev:,.0f} vs semana anterior: R$ {lw_rev:,.0f} ({wow:+.1f}%).",
                        f"{wow:+.1f}%")

            # 13. Best/worst day this month
            current_mo = date.today().strftime("%Y-%m")
            mo_days = [d for d in sorted_days if d.startswith(current_mo)]
            if mo_days:
                best_day = max(mo_days, key=lambda d: daily_svc[d].get("rev", 0))
                worst_day = min(mo_days, key=lambda d: daily_svc[d].get("rev", 0))
                best_rev = daily_svc[best_day].get("rev", 0)
                if best_rev > 0:
                    add("daily", "info", "Melhor/Pior Dia do Mes",
                        f"Melhor dia: {best_day} (R$ {best_rev:,.0f}). "
                        f"Pior dia: {worst_day} (R$ {daily_svc[worst_day].get('rev', 0):,.0f}).",
                        f"R$ {best_rev:,.0f}")

            # 14. Daily booking trend
            if len(recent_days) >= 14:
                first7_n = sum(daily_svc[d].get("n_conf", 0) for d in recent_days[-14:-7])
                last7_n = sum(daily_svc[d].get("n_conf", 0) for d in recent_days[-7:])
                if first7_n > 0:
                    trend = (last7_n - first7_n) / first7_n * 100
                    if abs(trend) > 20:
                        sev = "positive" if trend > 0 else "warning"
                        direction = "acelerando" if trend > 0 else "desacelerando"
                        add("daily", sev, f"Volume Diario {direction.title()}",
                            f"Bookings ultimos 7d: {last7_n} vs 7d anteriores: {first7_n} ({trend:+.0f}%).",
                            f"{trend:+.0f}%")

            # 15. Cancellation spike
            if len(recent_days) >= 7:
                canc_last7 = [daily_svc[d].get("n_canc", 0) for d in last7]
                max_canc_day = max(canc_last7)
                avg_canc = sum(canc_last7) / 7
                if max_canc_day > avg_canc * 3 and max_canc_day > 5:
                    spike_day = last7[canc_last7.index(max_canc_day)]
                    add("daily", "warning", "Pico de Cancelamentos",
                        f"{max_canc_day} cancelamentos em {spike_day} (media 7d: {avg_canc:.0f}).",
                        f"{max_canc_day} canc")

    # ---------------------------------------------------------------
    # MARGIN ALERTS (rules 16-20)
    # ---------------------------------------------------------------
    mt_months = sorted(mt.keys())
    if len(mt_months) >= 6:
        # 16. Margin trend last 3 vs prior 3
        last3_margins = [mt[m].get("margin_pct", 0) for m in mt_months[-3:]]
        prev3_margins = [mt[m].get("margin_pct", 0) for m in mt_months[-6:-3]]
        avg_last3 = sum(last3_margins) / 3
        avg_prev3 = sum(prev3_margins) / 3

        if avg_last3 < avg_prev3 * 0.85:
            add("alert", "critical", "Margem em Queda",
                f"Margem media caiu de {avg_prev3:.1f}% para {avg_last3:.1f}% "
                f"nos ultimos 3 meses (-{abs(avg_prev3 - avg_last3):.1f}pp).",
                f"{avg_last3:.1f}%")
        elif avg_last3 > avg_prev3 * 1.1:
            add("trend", "positive", "Margem em Alta",
                f"Margem media subiu de {avg_prev3:.1f}% para {avg_last3:.1f}%.",
                f"{avg_last3:.1f}%")

    if len(mt_months) >= 3:
        # 17. Markup below threshold
        last3_mkp = [mt[m].get("mkp_avg", 0) for m in mt_months[-3:]]
        avg_mkp = sum(last3_mkp) / 3
        if avg_mkp < 3.0:
            add("alert", "warning", "Markup Medio Baixo",
                f"Markup medio dos ultimos 3 meses: {avg_mkp:.1f}%. "
                f"Rever regras no EZConnect para integracoes com markup < 3%.",
                f"{avg_mkp:.1f}%")

    # 18. Negative margin services
    if len(neg) > 50:
        total_loss = sum(n.get("loss", 0) for n in neg)
        add("alert", "critical", f"{len(neg)} Servicos com Margem Negativa",
            f"Perda total de R$ {abs(total_loss):,.0f} em servicos com margem negativa.",
            f"R$ {abs(total_loss):,.0f}")
    elif len(neg) > 10:
        total_loss = sum(n.get("loss", 0) for n in neg)
        add("alert", "warning", f"{len(neg)} Servicos com Margem Negativa",
            f"Perda de R$ {abs(total_loss):,.0f}. Revisar regras de markup.",
            f"R$ {abs(total_loss):,.0f}")

    # 19. Provider-specific margin decline
    if mp:
        low_margin_high_vol = [(p, v) for p, v in mp.items()
                               if v.get("margin", 0) < 1 and v.get("n", 0) > 50]
        for p, v in sorted(low_margin_high_vol, key=lambda x: x[1]["rev"], reverse=True)[:3]:
            add("alert", "warning", f"Margem Critica: {p}",
                f"Provider com {v['n']:,} servicos e apenas {v['margin']:.1f}% de margem "
                f"(R$ {v['rev']:,.0f} revenue, R$ {v['profit']:,.0f} profit).",
                f"{v['margin']:.1f}%")

    # 20. Agency-specific margin outliers
    if ma and total_rev > 0:
        agency_outliers = [(a, v) for a, v in ma.items()
                          if v.get("margin", 0) < 0 and v.get("n", 0) > 20]
        for a, v in sorted(agency_outliers, key=lambda x: x[1]["profit"])[:3]:
            add("alert", "critical", f"Margem Negativa: {a}",
                f"Agencia com {v['n']:,} servicos e margem {v['margin']:.1f}% "
                f"(perda de R$ {abs(v['profit']):,.0f}).",
                f"{v['margin']:.1f}%")

    # ---------------------------------------------------------------
    # CASH FLOW (rules 21-23)
    # ---------------------------------------------------------------
    if months and len(months) >= 3:
        # 21. Cash flow negative months
        last3_net = [cf.get(m, {}).get("net_brl", 0) for m in months[-3:]]
        neg_months = sum(1 for n in last3_net if n < 0)
        if neg_months >= 2:
            add("alert", "critical", "Cash Flow Negativo",
                f"Cash flow BRL negativo em {neg_months} dos ultimos 3 meses. Risco de liquidez.",
                f"{neg_months}/3")

        # 22. DC net trend
        last3_dc = [cf.get(m, {}).get("dc_net", 0) for m in months[-3:]]
        avg_dc = sum(last3_dc) / 3
        if avg_dc < -50000:
            add("alert", "warning", "DebitCredit Net Negativo",
                f"Media DC net ultimos 3 meses: R$ {avg_dc:,.0f}. "
                f"Mais debitos que creditos no periodo.",
                f"R$ {avg_dc:,.0f}")

    # 23. AR aging critical
    aging_120 = aging.get("120d+", {}).get("amount", 0)
    aging_90 = aging.get("90d", {}).get("amount", 0)
    if aging_120 > 100000:
        add("alert", "critical", "AR Vencido 120+ dias",
            f"R$ {aging_120:,.0f} em contas a receber com mais de 120 dias.",
            f"R$ {aging_120:,.0f}")
    if aging_90 + aging_120 > 200000:
        add("alert", "warning", "AR Vencido 90+ dias",
            f"R$ {(aging_90 + aging_120):,.0f} em contas vencidas ha mais de 90 dias.",
            f"R$ {(aging_90 + aging_120):,.0f}")

    # ---------------------------------------------------------------
    # CANCELLATION (rules 24-26)
    # ---------------------------------------------------------------
    canc_months = sorted(canc.keys())
    if canc_months and months:
        # Use the same 3 months for both numerator and denominator
        last3 = months[-3:]
        last3_canc_n = sum(canc.get(m, {}).get("n", 0) for m in last3)
        last3_canc_lost = sum(canc.get(m, {}).get("lost_rev", 0) for m in last3)
        last3_total_n = sum(pnl.get(m, {}).get("n_total", 0) for m in last3)
        canc_rate = safe_div(last3_canc_n, last3_total_n) * 100
        # Sanity cap at 100%
        canc_rate = min(canc_rate, 100.0)

        # 24. Cancellation rate threshold
        if canc_rate > 15:
            add("alert", "critical", "Taxa de Cancelamento Alta",
                f"Taxa {canc_rate:.1f}% nos ultimos 3 meses. "
                f"Receita perdida: R$ {last3_canc_lost:,.0f}.",
                f"{canc_rate:.1f}%")
        elif canc_rate > 8:
            add("alert", "warning", "Cancelamentos Acima da Media",
                f"Taxa {canc_rate:.1f}% ({last3_canc_n:,} cancelamentos, R$ {last3_canc_lost:,.0f} perdidos).",
                f"{canc_rate:.1f}%")

        # 25. Cancellation trend increasing
        if len(canc_months) >= 6:
            last3_cr = [safe_div(canc.get(m, {}).get("n", 0), pnl.get(m, {}).get("n_total", 1)) * 100
                        for m in canc_months[-3:]]
            prev3_cr = [safe_div(canc.get(m, {}).get("n", 0), pnl.get(m, {}).get("n_total", 1)) * 100
                        for m in canc_months[-6:-3]]
            avg_last = sum(last3_cr) / 3
            avg_prev = sum(prev3_cr) / 3
            if avg_last > avg_prev * 1.3 and avg_last > 5:
                add("trend", "warning", "Cancelamentos em Tendencia de Alta",
                    f"Taxa media subiu de {avg_prev:.1f}% para {avg_last:.1f}%.",
                    f"{avg_last - avg_prev:+.1f}pp")

    # 26. Top agencies with high cancellation
    if canc_ag:
        for ag, v in sorted(canc_ag.items(), key=lambda x: x[1].get("n", 0), reverse=True)[:3]:
            rate = v.get("rate_pct", 0)
            if rate > 20 and v.get("n", 0) > 10:
                add("alert", "warning", f"Alto Cancelamento: {ag}",
                    f"{v['n']} cancelamentos ({rate:.0f}% da agencia), "
                    f"R$ {v.get('lost_rev', 0):,.0f} perdidos.",
                    f"{rate:.0f}%")

    # ---------------------------------------------------------------
    # COMPLIANCE (rules 27-30)
    # ---------------------------------------------------------------
    # 27. NF emission rate
    nf_pct = inv_s.get("nf_pct", 0)
    if nf_pct < 50:
        add("alert", "critical", "NF Emissao Critica",
            f"Apenas {nf_pct:.0f}% dos registros DC com nota fiscal. Risco fiscal.",
            f"{nf_pct:.0f}%")
    elif nf_pct < 80:
        add("alert", "warning", "NF Abaixo do Ideal",
            f"{nf_pct:.0f}% com NF. Meta: >90%.",
            f"{nf_pct:.0f}%")

    # 28. Invoice rate
    inv_pct_val = inv_s.get("invoiced_pct", 0)
    if inv_pct_val > 85:
        add("trend", "positive", "Faturamento Saudavel",
            f"{inv_pct_val:.0f}% dos registros faturados.",
            f"{inv_pct_val:.0f}%")
    elif inv_pct_val < 60:
        add("alert", "warning", "Baixo Faturamento",
            f"Apenas {inv_pct_val:.0f}% faturado. Agilizar emissao de faturas.",
            f"{inv_pct_val:.0f}%")

    # 29. DSO outliers
    dso_vals = [(ag, v.get("avg_dso", 0)) for ag, v in dso_ag.items() if v.get("n", 0) >= 5]
    if dso_vals:
        dso_vals_sorted = sorted(dso_vals, key=lambda x: x[1], reverse=True)
        overall_dso = sum(v for _, v in dso_vals) / len(dso_vals)
        for ag, dso in dso_vals_sorted[:3]:
            if dso > overall_dso * 2 and dso > 60:
                add("alert", "warning", f"DSO Alto: {ag}",
                    f"DSO de {dso:.0f} dias (media geral: {overall_dso:.0f} dias).",
                    f"{dso:.0f} dias")

    # 30. Payment deadline compliance
    if pay_deadline:
        recent_pd = sorted(pay_deadline.keys())[-3:]
        total_late = sum(pay_deadline[m].get("late", 0) for m in recent_pd)
        total_on_time = sum(pay_deadline[m].get("on_time", 0) for m in recent_pd)
        total_checks = total_late + total_on_time
        if total_checks > 0:
            late_pct = total_late / total_checks * 100
            if late_pct > 30:
                add("alert", "warning", "Pagamentos Atrasados",
                    f"{late_pct:.0f}% dos pagamentos atrasados nos ultimos 3 meses ({total_late} de {total_checks}).",
                    f"{late_pct:.0f}%")

    # ---------------------------------------------------------------
    # GEOGRAPHIC (rules 31-33)
    # ---------------------------------------------------------------
    if geo_total:
        # 31. Concentration risk
        sorted_countries = sorted(geo_total.items(), key=lambda x: x[1].get("rev", 0), reverse=True)
        if sorted_countries:
            top1_rev = sorted_countries[0][1].get("rev", 0)
            total_geo_rev = sum(v.get("rev", 0) for _, v in sorted_countries)
            if total_geo_rev > 0:
                top1_pct = top1_rev / total_geo_rev * 100
                if top1_pct > 80:
                    add("alert", "warning", f"Concentracao Geografica: {sorted_countries[0][0]}",
                        f"{top1_pct:.0f}% da receita concentrada em {sorted_countries[0][0]}. "
                        f"Diversificar mercados.",
                        f"{top1_pct:.0f}%")

        # 32. Country revenue shift
        geo_monthly = data.get("geo_country_monthly", {})
        geo_months = sorted(geo_monthly.keys())
        if len(geo_months) >= 6:
            recent_3 = geo_months[-3:]
            prior_3 = geo_months[-6:-3]
            for country in list(set(c for m in recent_3 for c in geo_monthly.get(m, {}).keys()))[:5]:
                recent_rev = sum(geo_monthly.get(m, {}).get(country, {}).get("rev", 0) for m in recent_3)
                prior_rev = sum(geo_monthly.get(m, {}).get(country, {}).get("rev", 0) for m in prior_3)
                if prior_rev > 10000 and recent_rev > 10000:
                    chg = (recent_rev - prior_rev) / prior_rev * 100
                    if abs(chg) > 30:
                        direction = "crescimento" if chg > 0 else "queda"
                        sev = "positive" if chg > 0 else "info"
                        add("trend", sev, f"Geo {direction.title()}: {country}",
                            f"{country}: R$ {recent_rev:,.0f} (3m recentes) vs R$ {prior_rev:,.0f} (3m anteriores, {chg:+.0f}%).",
                            f"{chg:+.0f}%")

        # 33. Emerging markets
        if len(sorted_countries) > 3:
            emerging = [(c, v) for c, v in sorted_countries[3:10]
                       if v.get("n", 0) > 20 and v.get("margin_pct", 0) > 5]
            if emerging:
                top_emerging = emerging[0]
                add("opportunity", "info", f"Mercado Emergente: {top_emerging[0]}",
                    f"{top_emerging[0]}: {top_emerging[1]['n']} servicos, margem {top_emerging[1]['margin_pct']:.1f}%, "
                    f"R$ {top_emerging[1]['rev']:,.0f} revenue. Potencial de crescimento.",
                    f"{top_emerging[1]['n']} servicos")

    # ---------------------------------------------------------------
    # OPERATIONAL (rules 34-40)
    # ---------------------------------------------------------------
    # 34. Revenue concentration top 5 agencies
    if ma and total_rev > 0:
        ma_sorted = sorted(ma.items(), key=lambda x: x[1].get("rev", 0), reverse=True)
        top5_rev = sum(v.get("rev", 0) for _, v in ma_sorted[:5])
        concentration = top5_rev / total_rev * 100
        if concentration > 60:
            top5_names = ", ".join(a for a, _ in ma_sorted[:5])
            add("alert", "warning", "Concentracao de Receita",
                f"Top 5 agencias: {concentration:.0f}% da receita ({top5_names}).",
                f"{concentration:.0f}%")

    # 35. Provider cost efficiency
    if mp:
        high_cost_low_margin = [(p, v) for p, v in mp.items()
                                if v.get("cost", 0) > 100000 and v.get("margin", 0) < 2]
        for p, v in sorted(high_cost_low_margin, key=lambda x: x[1]["cost"], reverse=True)[:2]:
            add("opportunity", "info", f"Custo Alto / Margem Baixa: {p}",
                f"R$ {v['cost']:,.0f} em custo com margem de apenas {v['margin']:.1f}%. "
                f"Negociar melhores condicoes.",
                f"R$ {v['cost']:,.0f}")

    # 36. Refund concentration
    ref_items = sorted(ref_ag.items(), key=lambda x: x[1].get("ref_c", 0), reverse=True)
    if ref_items and ref_items[0][1].get("ref_c", 0) > 50000:
        top_ref = ref_items[0]
        add("alert", "warning", f"Refunds Concentrados: {top_ref[0]}",
            f"R$ {top_ref[1]['ref_c']:,.0f} em refunds para cliente. Investigar causa.",
            f"R$ {top_ref[1]['ref_c']:,.0f}")

    # 37. Ticket volume trend
    tkt_months_sorted = sorted(tkt_monthly.keys())
    if len(tkt_months_sorted) >= 4:
        last2_tkt = sum(tkt_monthly.get(m, 0) for m in tkt_months_sorted[-2:])
        prev2_tkt = sum(tkt_monthly.get(m, 0) for m in tkt_months_sorted[-4:-2])
        if prev2_tkt > 0:
            tkt_chg = (last2_tkt - prev2_tkt) / prev2_tkt * 100
            if tkt_chg > 30:
                add("trend", "warning", "Tickets em Alta",
                    f"{last2_tkt} tickets nos ultimos 2 meses vs {prev2_tkt} nos 2 anteriores ({tkt_chg:+.0f}%).",
                    f"{tkt_chg:+.0f}%")

    # 38. Average booking value trend
    if len(months) >= 6:
        last3_avg = [safe_div(pnl[m].get("revenue_gross", 0), pnl[m].get("n_conf", 1))
                     for m in months[-3:]]
        prev3_avg = [safe_div(pnl[m].get("revenue_gross", 0), pnl[m].get("n_conf", 1))
                     for m in months[-6:-3]]
        avg_recent = sum(last3_avg) / 3
        avg_prior = sum(prev3_avg) / 3
        if avg_prior > 0:
            chg = (avg_recent - avg_prior) / avg_prior * 100
            if abs(chg) > 15:
                direction = "subiu" if chg > 0 else "caiu"
                sev = "positive" if chg > 0 else "warning"
                add("trend", sev, f"Ticket Medio {direction.title()}",
                    f"Ticket medio {direction} de R$ {avg_prior:,.0f} para R$ {avg_recent:,.0f} ({chg:+.0f}%).",
                    f"{chg:+.0f}%")

    # 39. Room night trends
    rn_months_sorted = sorted(rn_monthly.keys())
    if len(rn_months_sorted) >= 4:
        last2_rn = sum(rn_monthly[m].get("total_nights", 0) for m in rn_months_sorted[-2:])
        prev2_rn = sum(rn_monthly[m].get("total_nights", 0) for m in rn_months_sorted[-4:-2])
        if prev2_rn > 0:
            rn_chg = (last2_rn - prev2_rn) / prev2_rn * 100
            if abs(rn_chg) > 20:
                direction = "cresceram" if rn_chg > 0 else "cairam"
                sev = "positive" if rn_chg > 0 else "warning"
                add("trend", sev, f"Room Nights {direction.title()}",
                    f"{last2_rn:,} room-nights (2m recentes) vs {prev2_rn:,} (2m anteriores, {rn_chg:+.0f}%).",
                    f"{rn_chg:+.0f}%")

    # 40. Lead time changes
    lt_months_sorted = sorted(lt_monthly.keys())
    if len(lt_months_sorted) >= 4:
        last2_lt = [lt_monthly[m].get("avg_days", 0) for m in lt_months_sorted[-2:]]
        prev2_lt = [lt_monthly[m].get("avg_days", 0) for m in lt_months_sorted[-4:-2]]
        avg_lt_recent = sum(last2_lt) / 2
        avg_lt_prior = sum(prev2_lt) / 2
        if avg_lt_prior > 0:
            lt_chg = avg_lt_recent - avg_lt_prior
            if abs(lt_chg) > 10:
                direction = "aumentou" if lt_chg > 0 else "diminuiu"
                add("trend", "info", f"Lead Time {direction.title()}",
                    f"Lead time medio {direction} de {avg_lt_prior:.0f} para {avg_lt_recent:.0f} dias.",
                    f"{lt_chg:+.0f} dias")

    # ---------------------------------------------------------------
    # OPPORTUNITIES (rules 41-43)
    # ---------------------------------------------------------------
    # 41. Low-margin high-volume providers
    if mp:
        for p, v in sorted(mp.items(), key=lambda x: x[1].get("rev", 0), reverse=True)[:10]:
            if v.get("margin", 0) < 2 and v.get("n", 0) > 100:
                add("opportunity", "info", f"Oportunidade Markup: {p}",
                    f"{v['n']:,} servicos, margem {v['margin']:.1f}%. "
                    f"Aumentar markup pode gerar +R$ {v['rev'] * 0.02:,.0f} em profit.",
                    f"{v['margin']:.1f}%")
                break

    # 42. Agencies with improving margins
    if ma and len(months) >= 6:
        for a, v in sorted(ma.items(), key=lambda x: x[1].get("rev", 0), reverse=True)[:20]:
            if v.get("margin", 0) > 5 and v.get("n", 0) > 50:
                add("opportunity", "positive", f"Margem Forte: {a}",
                    f"Agencia com margem {v['margin']:.1f}% e {v['n']:,} servicos. "
                    f"Potencial para crescer volume.",
                    f"{v['margin']:.1f}%")
                break

    # 43. Geographic opportunity
    if geo_total:
        intl = {c: v for c, v in geo_total.items() if c != "BR" and v.get("n", 0) > 10}
        if intl:
            best_intl = max(intl.items(), key=lambda x: x[1].get("margin_pct", 0))
            if best_intl[1].get("margin_pct", 0) > 5:
                add("opportunity", "info", f"Mercado Internacional: {best_intl[0]}",
                    f"{best_intl[0]}: margem {best_intl[1]['margin_pct']:.1f}% com {best_intl[1]['n']} servicos. "
                    f"Explorar crescimento.",
                    f"{best_intl[1]['margin_pct']:.1f}%")

    # ---------------------------------------------------------------
    # REVENUE GROWTH (extra)
    # ---------------------------------------------------------------
    if months and len(months) >= 6:
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
                    f"Revenue medio mensal caiu {abs(growth):.0f}%.",
                    f"{growth:.0f}%")

    # Sort: critical first, then warning, info, positive
    sev_order = {"critical": 0, "warning": 1, "info": 2, "positive": 3}
    insights.sort(key=lambda x: sev_order.get(x["sev"], 9))

    return insights


# ============================================================================
# MAIN BUILD FUNCTION
# ============================================================================

def build():
    print("=" * 70)
    print("  build_financial_data.py v3 — CR FINANCIAL EZLINK")
    print("=" * 70)

    today_dt = datetime.now()

    # ====================================================================
    # 1. LOAD SERVICES (Created first for priority, then legacy)
    # ====================================================================
    print("\n[1/4] Loading Services CSVs...")
    created_files = sorted(
        glob.glob(os.path.join(SERVICES_CREATED_DIR, "CR_Services_Created_*.csv"))
    )
    legacy_files = sorted(
        glob.glob(os.path.join(SERVICES_DIR, "CR_Services_*.csv"))
    )
    svc_files = created_files + legacy_files
    print(f"  Found {len(created_files)} created + {len(legacy_files)} legacy files")

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
    print(f"  Total unique services: {len(services):,}")

    # ====================================================================
    # 2. LOAD DEBITCREDIT
    # ====================================================================
    print("\n[2/4] Loading DebitCredit CSVs...")
    dc_files = sorted(
        glob.glob(os.path.join(DEBITCREDIT_DIR, "CR_DebitCredit_*.csv"))
    )
    print(f"  Found {len(dc_files)} files")

    dc_rows = []
    dc_seen = set()
    for fpath in dc_files:
        fname = os.path.basename(fpath)
        try:
            with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f, delimiter=",")
                for row in reader:
                    dcid = (row.get("id") or "").strip().strip('"')
                    if dcid and dcid not in dc_seen:
                        dc_seen.add(dcid)
                        dc_rows.append(row)
        except Exception as e:
            print(f"  WARNING: {fname}: {e}")
    print(f"  Total unique DC rows: {len(dc_rows):,}")

    # ====================================================================
    # 3. LOAD TICKETS
    # ====================================================================
    print("\n[3/4] Loading Tickets CSVs...")
    tkt_files = sorted(
        glob.glob(os.path.join(TICKETS_DIR, "CR_Tickets_*.csv"))
    )
    print(f"  Found {len(tkt_files)} files")

    tickets = {}
    for fpath in tkt_files:
        fname = os.path.basename(fpath)
        try:
            with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    tid = (row.get("id") or "").strip().strip('"')
                    if tid and tid not in tickets:
                        tickets[tid] = row
        except Exception as e:
            print(f"  WARNING: {fname}: {e}")
    print(f"  Total unique tickets: {len(tickets):,}")

    # ====================================================================
    # 4. AGGREGATE ALL FINANCIAL DATA
    # ====================================================================
    print("\n[4/4] Aggregating financial data...")

    # --- P&L monthly ---
    pnl_monthly = defaultdict(lambda: {
        "revenue_gross": 0.0, "cost_provider": 0.0, "gross_profit": 0.0,
        "commissions": 0.0, "refunds_client": 0.0, "refunds_provider": 0.0,
        "cancellation_cost": 0.0, "net_operating": 0.0,
        "n_total": 0, "n_conf": 0, "n_canc": 0, "n_in_progress": 0,
        "avg_ticket": 0.0, "margin_pct": 0.0,
        "room_nights": 0, "lead_time_sum": 0.0, "lead_time_n": 0,
    })

    # --- Cash Flow ---
    cf_monthly = defaultdict(lambda: {
        "inflows_brl": 0.0, "outflows_brl": 0.0, "net_brl": 0.0,
        "inflows_usd": 0.0, "outflows_usd": 0.0, "net_usd": 0.0,
        "dc_credits": 0.0, "dc_debits": 0.0, "dc_net": 0.0,
    })
    cf_daily_data = defaultdict(lambda: [0.0, 0.0])  # [credits, debits]

    # --- Revenue Recognition (3 timelines) ---
    rev_svc_date = defaultdict(lambda: {"gross": 0.0, "cost": 0.0, "profit": 0.0, "n": 0})
    rev_created = defaultdict(lambda: {"gross": 0.0, "cost": 0.0, "profit": 0.0, "n": 0})
    rev_finan = defaultdict(lambda: {"gross": 0.0, "credits": 0.0, "debits": 0.0, "n": 0})

    # --- AR / AP ---
    ar_agency = defaultdict(lambda: {
        "total_sale": 0.0, "invoiced": 0.0, "not_invoiced": 0.0,
        "n_services": 0, "n_invoiced": 0,
    })
    ar_aging = {"current": [0.0, 0], "30d": [0.0, 0], "60d": [0.0, 0],
                "90d": [0.0, 0], "120d+": [0.0, 0]}
    ar_aging_ag = defaultdict(lambda: {"current": 0.0, "30d": 0.0, "60d": 0.0,
                                        "90d": 0.0, "120d+": 0.0})
    ap_provider = defaultdict(lambda: {
        "total_cost": 0.0, "n_services": 0,
        "currencies": defaultdict(float),
    })
    ap_monthly = defaultdict(lambda: defaultdict(lambda: {"cost": 0.0, "n": 0}))

    # --- P&L by entity ---
    pnl_agency = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0, "comm": 0.0,
        "ref_c": 0.0, "ref_p": 0.0, "canc_cost": 0.0, "n": 0,
    }))
    pnl_provider = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0, "n": 0,
    }))

    # --- Currency monthly ---
    curr_monthly = defaultdict(lambda: defaultdict(lambda: {
        "sale": 0.0, "cost": 0.0, "profit": 0.0, "n": 0,
        "rate_sum": 0.0, "rate_n": 0,
    }))

    # --- Payment / Source mix ---
    pay_monthly = defaultdict(lambda: defaultdict(lambda: {"n": 0, "amount": 0.0}))
    src_monthly = defaultdict(lambda: defaultdict(lambda: {"n": 0, "amount": 0.0}))

    # --- Commission ---
    comm_agency = defaultdict(lambda: {"total": 0.0, "n": 0})
    comm_monthly = defaultdict(lambda: {"total": 0.0, "n": 0})

    # --- Refunds ---
    ref_monthly = defaultdict(lambda: {"ref_client": 0.0, "ref_provider": 0.0, "n": 0})
    ref_agency = defaultdict(lambda: {"ref_c": 0.0, "ref_p": 0.0, "n": 0})
    ref_provider = defaultdict(lambda: {"ref_c": 0.0, "ref_p": 0.0, "n": 0})

    # --- Disputes ---
    disp_monthly = defaultdict(lambda: {"n": 0})
    disp_agency = defaultdict(int)
    disp_hotel = defaultdict(int)

    # --- Invoice / NF / DSO (from DC) ---
    inv_monthly = defaultdict(lambda: {
        "invoiced_n": 0, "not_invoiced_n": 0,
        "invoiced_amt": 0.0, "not_invoiced_amt": 0.0,
        "nf_emitted_n": 0, "nf_pending_n": 0,
    })
    nf_agency = defaultdict(lambda: {"with_nf": 0, "without_nf": 0})
    dso_agency = defaultdict(lambda: [0.0, 0])
    dso_monthly_acc = defaultdict(lambda: [0.0, 0])

    # --- Margin ---
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
    neg_margin = []

    # --- Cancellation ---
    canc_monthly = defaultdict(lambda: {
        "n": 0, "lost_rev": 0.0, "penalties": 0.0, "refunds_paid": 0.0, "rate_pct": 0.0,
    })
    canc_agency = defaultdict(lambda: {
        "n": 0, "lost_rev": 0.0, "penalties": 0.0, "rate_pct": 0.0, "n_total": 0,
    })

    # --- Upcoming ---
    upcoming = defaultdict(lambda: {"value": 0.0, "n": 0})

    # --- YoY ---
    yoy_acc = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "profit": 0.0, "cost": 0.0,
        "n_conf": 0, "n_canc": 0,
        "commissions": 0.0, "refunds": 0.0,
        "room_nights": 0, "ticket_sum": 0.0, "ticket_n": 0,
    }))

    # --- Geographic ---
    geo_country_monthly = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "n": 0, "profit": 0.0,
    }))
    geo_country_total = defaultdict(lambda: {
        "rev": 0.0, "n": 0, "profit": 0.0, "cost": 0.0,
    })
    geo_city_acc = defaultdict(lambda: {
        "rev": 0.0, "n": 0, "profit": 0.0, "country": "",
    })

    # --- Room Nights ---
    rn_monthly = defaultdict(lambda: {
        "total_nights": 0, "n_bookings": 0,
    })

    # --- Lead Time ---
    lt_monthly = defaultdict(lambda: {"sum_days": 0.0, "n": 0})
    lt_agency = defaultdict(lambda: {"sum_days": 0.0, "n": 0})

    # --- Integration ---
    integ_monthly = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "n": 0, "profit": 0.0,
    }))
    integ_total = defaultdict(lambda: {
        "rev": 0.0, "n": 0, "profit": 0.0, "cost": 0.0,
    })

    # --- Agency ranking monthly ---
    agency_rank_monthly = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "profit": 0.0, "n": 0,
    }))

    # --- Daily data (current year only) ---
    daily_svc = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "n_conf": 0, "n_canc": 0, "n_total": 0,
        "refunds": 0.0, "comm": 0.0,
        "room_nights": 0,
    })
    daily_created = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "n_conf": 0, "n_canc": 0, "n_total": 0,
        "refunds": 0.0, "comm": 0.0,
        "room_nights": 0,
    })
    daily_top_agencies_acc = defaultdict(lambda: defaultdict(lambda: {
        "rev": 0.0, "n": 0,
    }))

    # --- Weekly rollup (current year) ---
    weekly_acc = defaultdict(lambda: {
        "rev": 0.0, "cost": 0.0, "profit": 0.0,
        "n_conf": 0, "n_canc": 0,
        "room_nights": 0,
        "mkp_sum": 0.0, "mkp_n": 0,
        "week_start": "", "week_end": "",
    })

    # --- Protection / Refundable ---
    prot_monthly = defaultdict(lambda: {"protected_n": 0, "unprotected_n": 0, "protected_rev": 0.0})
    refundable_monthly = defaultdict(lambda: {"refundable_n": 0, "non_refundable_n": 0})

    # --- Booking Value Distribution ---
    bvd_counts = [0] * 8
    bvd_revenue = [0.0] * 8

    # --- Payment Deadline Compliance ---
    pay_deadline = defaultdict(lambda: {"on_time": 0, "late": 0, "no_deadline": 0})

    # --- Agency cancellation totals (for rate) ---
    agency_total_count = defaultdict(int)

    # ====================================================================
    # PROCESS SERVICES
    # ====================================================================
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
        integration = (row.get("integration") or "").strip().strip('"')
        city = (row.get("city") or "").strip().strip('"')
        country_iso = (row.get("country_iso2") or "").strip().strip('"').upper()
        country_name = (row.get("country") or "").strip().strip('"')

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
        room_nights_val = parse_int(row.get("room_number_nights", "0"))
        protection = is_truthy(row.get("protection", ""))
        refundable = is_truthy(row.get("refundable", ""))

        service_date = parse_date(row.get("service_date", ""))
        created_at = parse_date(row.get("created_at", ""))
        payment_deadline_dt = parse_date(row.get("payment_deadline", ""))

        # Filtro de ano:
        #   - Nunca aceita service_date anterior a MIN_YEAR (ex: 2024 pra tras).
        #   - Aceita service_date posterior a MAX_YEAR (ex: 2027+) se o booking foi
        #     criado em MIN_YEAR..MAX_YEAR (ex: future booking criado em 2026).
        #   - Se nao tem service_date, usa created_at.
        sd_year = service_date.year if service_date else None
        ca_year = created_at.year if created_at else None
        if sd_year is not None:
            if sd_year < MIN_YEAR:
                continue
            if sd_year > MAX_YEAR:
                # Future service: so mantem se criado no periodo
                if ca_year is None or not (MIN_YEAR <= ca_year <= MAX_YEAR):
                    continue
        else:
            # Sem service_date: usa created_at
            if ca_year is None or not (MIN_YEAR <= ca_year <= MAX_YEAR):
                continue

        # BRL conversion
        if exchange_rate > 1:
            sale_brl = price_sale * exchange_rate
            cost_brl = price_provider * exchange_rate
        else:
            sale_brl = price_sale
            cost_brl = price_provider

        # Profit formula: NEVER use profit_brl field
        profit = cost_brl * markup_val / 100.0

        sd_mo = month_key(service_date)
        ca_mo = month_key(created_at)

        if not agency:
            continue

        svc_count += 1

        # Use country_iso if available, fallback to country name
        geo_key = country_iso if country_iso and len(country_iso) == 2 else country_name[:20] if country_name else ""

        # Lead time calculation
        lead_days = None
        if service_date and created_at:
            ld = (service_date - created_at).days
            if 0 <= ld <= 730:
                lead_days = ld

        # ---- Revenue Recognition ----
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

        # ---- P&L Monthly (by service_date) ----
        if sd_mo:
            p = pnl_monthly[sd_mo]
            p["n_total"] += 1
            if sc == "C":
                p["revenue_gross"] += sale_brl
                p["cost_provider"] += cost_brl
                p["gross_profit"] += profit
                p["commissions"] += commission
                p["n_conf"] += 1
                p["room_nights"] += room_nights_val
                if lead_days is not None:
                    p["lead_time_sum"] += lead_days
                    p["lead_time_n"] += 1
            elif sc == "X":
                p["cancellation_cost"] += sale_brl
                p["n_canc"] += 1
            elif sc == "I":
                p["n_in_progress"] += 1
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

        # ---- Cash Flow (confirmed = inflow) ----
        if sd_mo and sc == "C":
            c = cf_monthly[sd_mo]
            if currency and currency.upper() == "USD":
                c["inflows_usd"] += price_sale
                c["outflows_usd"] += price_provider
            else:
                c["inflows_brl"] += sale_brl
                c["outflows_brl"] += cost_brl

        # ---- AR by agency (confirmed) ----
        if sc == "C" and agency:
            a = ar_agency[agency]
            a["total_sale"] += sale_brl
            a["n_services"] += 1
            if invoiced == 1:
                a["invoiced"] += sale_brl
                a["n_invoiced"] += 1
            else:
                a["not_invoiced"] += sale_brl

            # AR aging
            if invoiced == 0 and service_date:
                age_days = max(0, (today_dt - service_date).days)
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

        # ---- AP by provider ----
        if sc == "C" and provider:
            ap = ap_provider[provider]
            ap["total_cost"] += cost_brl
            ap["n_services"] += 1
            ap["currencies"][currency or "BRL"] += cost_brl
            if sd_mo:
                apm = ap_monthly[sd_mo][provider]
                apm["cost"] += cost_brl
                apm["n"] += 1

        # ---- Currency monthly ----
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

        # ---- Payment / Source mix ----
        if sd_mo and sc == "C":
            if payment_type:
                pm = pay_monthly[sd_mo][payment_type]
                pm["n"] += 1
                pm["amount"] += sale_brl
            if source:
                sm = src_monthly[sd_mo][source]
                sm["n"] += 1
                sm["amount"] += sale_brl

        # ---- Commission ----
        if commission > 0 and sc == "C":
            comm_agency[agency]["total"] += commission
            comm_agency[agency]["n"] += 1
            if sd_mo:
                comm_monthly[sd_mo]["total"] += commission
                comm_monthly[sd_mo]["n"] += 1

        # ---- Refunds ----
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

        # ---- Disputes ----
        if dispute and dispute not in ("", "0", "false", "False"):
            if sd_mo:
                disp_monthly[sd_mo]["n"] += 1
            if agency:
                disp_agency[agency] += 1
            if hotel:
                disp_hotel[hotel] += 1

        # ---- Margin (confirmed only) ----
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

        # ---- Cancellation ----
        agency_total_count[agency] += 1
        if sc == "X" and sd_mo:
            cm_c = canc_monthly[sd_mo]
            cm_c["n"] += 1
            cm_c["lost_rev"] += sale_brl
            cm_c["penalties"] += canc_price
            cm_c["refunds_paid"] += refund_client
            ca_c = canc_agency[agency]
            ca_c["n"] += 1
            ca_c["n_total"] += 1
            ca_c["lost_rev"] += sale_brl
            ca_c["penalties"] += canc_price
        else:
            canc_agency[agency]["n_total"] += 1

        # ---- Upcoming check-ins ----
        if sc == "C" and service_date and service_date > today_dt:
            umo = month_key(service_date)
            if umo:
                upcoming[umo]["value"] += sale_brl
                upcoming[umo]["n"] += 1

        # ---- YoY (by service_date, confirmed only for rev/profit) ----
        if service_date:
            yoy_month = service_date.strftime("%m")
            yoy_year = service_date.strftime("%Y")
            ya = yoy_acc[yoy_month][yoy_year]
            if sc == "C":
                ya["rev"] += sale_brl
                ya["profit"] += profit
                ya["cost"] += cost_brl
                ya["n_conf"] += 1
                ya["commissions"] += commission
                ya["refunds"] += refund_client
                ya["room_nights"] += room_nights_val
                ya["ticket_sum"] += sale_brl
                ya["ticket_n"] += 1
            elif sc == "X":
                ya["n_canc"] += 1

        # ---- Geographic ----
        if sc == "C" and geo_key:
            if sd_mo:
                g = geo_country_monthly[sd_mo][geo_key]
                g["rev"] += sale_brl
                g["n"] += 1
                g["profit"] += profit
            gt = geo_country_total[geo_key]
            gt["rev"] += sale_brl
            gt["n"] += 1
            gt["profit"] += profit
            gt["cost"] += cost_brl

            if city:
                gc = geo_city_acc[city]
                gc["rev"] += sale_brl
                gc["n"] += 1
                gc["profit"] += profit
                if not gc["country"]:
                    gc["country"] = geo_key

        # ---- Room Nights (confirmed) ----
        if sc == "C" and sd_mo and room_nights_val > 0:
            rn = rn_monthly[sd_mo]
            rn["total_nights"] += room_nights_val
            rn["n_bookings"] += 1

        # ---- Lead Time ----
        if sc == "C" and lead_days is not None and sd_mo:
            lt_monthly[sd_mo]["sum_days"] += lead_days
            lt_monthly[sd_mo]["n"] += 1
            if agency:
                lt_agency[agency]["sum_days"] += lead_days
                lt_agency[agency]["n"] += 1

        # ---- Integration ----
        if sc == "C" and integration and sd_mo:
            ig = integ_monthly[sd_mo][integration]
            ig["rev"] += sale_brl
            ig["n"] += 1
            ig["profit"] += profit
            it = integ_total[integration]
            it["rev"] += sale_brl
            it["n"] += 1
            it["profit"] += profit
            it["cost"] += cost_brl

        # ---- Agency ranking monthly (confirmed) ----
        if sc == "C" and sd_mo:
            arm = agency_rank_monthly[sd_mo][agency]
            arm["rev"] += sale_brl
            arm["profit"] += profit
            arm["n"] += 1

        # ---- Daily data (current year only) ----
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
                d["room_nights"] += room_nights_val
            elif sc == "X":
                d["n_canc"] += 1
            d["refunds"] += refund_client

            # Daily top agencies
            if sc == "C":
                dta = daily_top_agencies_acc[sd_day][agency]
                dta["rev"] += sale_brl
                dta["n"] += 1

        if ca_day and ca_day.startswith(CURRENT_YEAR):
            d = daily_created[ca_day]
            d["n_total"] += 1
            if sc == "C":
                d["rev"] += sale_brl
                d["cost"] += cost_brl
                d["profit"] += profit
                d["n_conf"] += 1
                d["comm"] += commission
                d["room_nights"] += room_nights_val
            elif sc == "X":
                d["n_canc"] += 1
            d["refunds"] += refund_client

        # ---- Weekly rollup (current year, by service_date) ----
        if service_date and service_date.strftime("%Y") == CURRENT_YEAR:
            wk_key, wk_start, wk_end = get_week_key(service_date)
            w = weekly_acc[wk_key]
            w["week_start"] = wk_start
            w["week_end"] = wk_end
            if sc == "C":
                w["rev"] += sale_brl
                w["cost"] += cost_brl
                w["profit"] += profit
                w["n_conf"] += 1
                w["room_nights"] += room_nights_val
                if markup_val != 0:
                    w["mkp_sum"] += markup_val
                    w["mkp_n"] += 1
            elif sc == "X":
                w["n_canc"] += 1

        # ---- Protection / Refundable ----
        if sd_mo:
            if protection:
                prot_monthly[sd_mo]["protected_n"] += 1
                if sc == "C":
                    prot_monthly[sd_mo]["protected_rev"] += sale_brl
            else:
                prot_monthly[sd_mo]["unprotected_n"] += 1
            if refundable:
                refundable_monthly[sd_mo]["refundable_n"] += 1
            else:
                refundable_monthly[sd_mo]["non_refundable_n"] += 1

        # ---- Booking Value Distribution (confirmed only) ----
        if sc == "C" and sale_brl > 0:
            idx = bucket_value(sale_brl)
            bvd_counts[idx] += 1
            bvd_revenue[idx] += sale_brl

        # ---- Payment Deadline Compliance ----
        if sd_mo and sc == "C":
            if payment_deadline_dt:
                if today_dt <= payment_deadline_dt or invoiced == 1:
                    pay_deadline[sd_mo]["on_time"] += 1
                else:
                    pay_deadline[sd_mo]["late"] += 1
            else:
                pay_deadline[sd_mo]["no_deadline"] += 1

    print(f"  Processed {svc_count:,} services")

    # ====================================================================
    # PROCESS DEBITCREDIT
    # ====================================================================
    print("  Processing DebitCredit...")
    dc_count = 0
    for row in dc_rows:
        finan_date = parse_date(row.get("finan_date", ""))
        finan_type = parse_int(row.get("finan_type", "0"))
        finan_net = parse_float(row.get("finan_net", "0"))
        finan_sale = parse_float(row.get("finan_sale", "0"))
        finan_status = parse_int(row.get("finan_status", "0"))
        agency = (row.get("agency_name") or "").strip().strip('"')
        provider = (row.get("provider_name") or "").strip().strip('"')
        invoiced_val = parse_int(row.get("invoiced", "0"))
        nf_val = parse_int(row.get("nf", "0"))
        price_sale_dc = parse_float(row.get("price_sale", "0"))
        reservation_date = parse_date(row.get("reservation_date", ""))

        fd_mo = month_key(finan_date)
        fd_day = date_key(finan_date)

        if not fd_mo:
            continue

        # Filtro de ano: somente MIN_YEAR..MAX_YEAR
        if finan_date and (finan_date.year < MIN_YEAR or finan_date.year > MAX_YEAR):
            continue

        dc_count += 1

        # Cash Flow from DC
        c = cf_monthly[fd_mo]
        if finan_type == 1:
            c["dc_credits"] += finan_net
        elif finan_type == 2:
            c["dc_debits"] += finan_net

        # Daily cash flow (current year only for daily)
        if fd_day:
            d = cf_daily_data[fd_day]
            if finan_type == 1:
                d[0] += finan_net
            elif finan_type == 2:
                d[1] += finan_net

        # Revenue by finan_date
        r = rev_finan[fd_mo]
        r["gross"] += price_sale_dc
        if finan_type == 1:
            r["credits"] += finan_net
        elif finan_type == 2:
            r["debits"] += finan_net
        r["n"] += 1

        # Invoice/NF
        iv = inv_monthly[fd_mo]
        if invoiced_val == 1:
            iv["invoiced_n"] += 1
            iv["invoiced_amt"] += price_sale_dc
        else:
            iv["not_invoiced_n"] += 1
            iv["not_invoiced_amt"] += price_sale_dc
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
                dso_monthly_acc[fd_mo][0] += dso_days
                dso_monthly_acc[fd_mo][1] += 1

    print(f"  Processed {dc_count:,} DC rows")

    # ====================================================================
    # PROCESS TICKETS (deep analysis)
    # ====================================================================
    print("  Processing Tickets...")
    tkt_monthly_count = defaultdict(int)
    tkt_by_priority = defaultdict(int)
    tkt_by_department = defaultdict(int)
    tkt_by_status = defaultdict(int)
    tkt_by_type = defaultdict(int)
    tkt_by_provider = defaultdict(int)
    tkt_resolution_monthly = defaultdict(lambda: {"sum_hours": 0.0, "n_closed": 0})
    tkt_fees_monthly = defaultdict(lambda: {"total_fees": 0.0, "n": 0})

    for tid, row in tickets.items():
        created = parse_date(row.get("created_at", ""))
        closed = parse_date(row.get("closed_at", ""))

        # Filtro de ano: somente tickets criados entre MIN_YEAR..MAX_YEAR
        if created and (created.year < MIN_YEAR or created.year > MAX_YEAR):
            continue

        tmo = month_key(created)
        if tmo:
            tkt_monthly_count[tmo] += 1

        # Priority
        prio = (row.get("priority") or "").strip().strip('"').lower()
        if prio in ("high", "alta", "urgent", "urgente"):
            tkt_by_priority["high"] += 1
        elif prio in ("medium", "media", "normal"):
            tkt_by_priority["medium"] += 1
        elif prio in ("low", "baixa"):
            tkt_by_priority["low"] += 1
        else:
            tkt_by_priority["none"] += 1

        # Department
        dept = (row.get("departament") or "").strip().strip('"')
        if dept:
            tkt_by_department[dept] += 1

        # Status
        tkt_status = (row.get("status") or "").strip().strip('"').lower()
        if "close" in tkt_status or "closed" in tkt_status:
            tkt_by_status["closed"] += 1
        elif "open" in tkt_status:
            tkt_by_status["open"] += 1
        elif "progress" in tkt_status or "pending" in tkt_status:
            tkt_by_status["in_progress"] += 1
        else:
            tkt_by_status[tkt_status or "other"] += 1

        # Type
        tkt_type = (row.get("type") or "").strip().strip('"')
        if tkt_type:
            tkt_by_type[tkt_type] += 1

        # Provider
        tkt_provider = (row.get("provider") or "").strip().strip('"')
        if tkt_provider:
            tkt_by_provider[tkt_provider] += 1

        # Resolution time
        if created and closed:
            hours = (closed - created).total_seconds() / 3600.0
            if 0 < hours < 8760:  # max 1 year
                cmo = month_key(closed)
                if cmo:
                    tkt_resolution_monthly[cmo]["sum_hours"] += hours
                    tkt_resolution_monthly[cmo]["n_closed"] += 1

        # Fees
        fee = parse_float(row.get("fee_price", "0"))
        charges = parse_float(row.get("charges", "0"))
        total_fee = fee + charges
        if total_fee > 0 and tmo:
            tkt_fees_monthly[tmo]["total_fees"] += total_fee
            tkt_fees_monthly[tmo]["n"] += 1

    print(f"  Processed {len(tickets):,} tickets")

    # ====================================================================
    # FINALIZE CALCULATIONS
    # ====================================================================
    print("\n  Finalizing calculations...")

    # Finalize P&L monthly
    for mo, p in pnl_monthly.items():
        p["net_operating"] = p["gross_profit"] - p["commissions"] - p["refunds_client"]
        if p["n_conf"] > 0:
            p["avg_ticket"] = r2(p["revenue_gross"] / p["n_conf"])
        if p["revenue_gross"] > 0:
            p["margin_pct"] = r2(p["gross_profit"] / p["revenue_gross"] * 100)
        if p["lead_time_n"] > 0:
            p["avg_lead_time"] = r2(p["lead_time_sum"] / p["lead_time_n"])
        else:
            p["avg_lead_time"] = 0.0
        # Remove accumulators from output
        del p["lead_time_sum"]
        del p["lead_time_n"]

    # Finalize cash flow net
    for mo, c in cf_monthly.items():
        c["net_brl"] = c["inflows_brl"] - c["outflows_brl"]
        c["net_usd"] = c["inflows_usd"] - c["outflows_usd"]
        c["dc_net"] = c["dc_credits"] - c["dc_debits"]

    # Finalize cancellation monthly rate
    for mo in canc_monthly:
        total_n = pnl_monthly[mo]["n_total"] if mo in pnl_monthly else 0
        rate = safe_div(canc_monthly[mo]["n"], total_n) * 100
        canc_monthly[mo]["rate_pct"] = r2(min(rate, 100.0))

    # Finalize cancellation by agency rate
    for ag in canc_agency:
        total = canc_agency[ag]["n_total"]
        canc_agency[ag]["rate_pct"] = r2(safe_div(canc_agency[ag]["n"], total) * 100)

    # ====================================================================
    # BUILD OUTPUT JSON
    # ====================================================================
    print("  Building output JSON...")

    # Sort negative margin by loss
    neg_margin.sort(key=lambda x: x["loss"])

    # Cash flow daily
    cf_daily_sorted = sorted(cf_daily_data.keys())
    cf_daily_out = {
        "d": cf_daily_sorted,
        "credits": [r2(cf_daily_data[d][0]) for d in cf_daily_sorted],
        "debits": [r2(cf_daily_data[d][1]) for d in cf_daily_sorted],
        "net": [r2(cf_daily_data[d][0] - cf_daily_data[d][1]) for d in cf_daily_sorted],
    }

    # AR aging output
    ar_aging_out = {}
    for bk in ["current", "30d", "60d", "90d", "120d+"]:
        ar_aging_out[bk] = {"amount": r2(ar_aging[bk][0]), "count": ar_aging[bk][1]}

    # AR aging by agency (top 50)
    ar_aging_ag_out = {}
    top_ar_agencies = sorted(ar_aging_ag.keys(),
                             key=lambda a: sum(ar_aging_ag[a].values()), reverse=True)[:TOP_AGENCIES]
    for ag in top_ar_agencies:
        ar_aging_ag_out[ag] = {k: r2(v) for k, v in ar_aging_ag[ag].items()}

    # AP provider output (top 50)
    ap_out = {}
    top_ap = sorted(ap_provider.keys(), key=lambda p: ap_provider[p]["total_cost"], reverse=True)[:TOP_PROVIDERS]
    for p in top_ap:
        d = ap_provider[p]
        ap_out[p] = {
            "total_cost": r2(d["total_cost"]),
            "n_services": d["n_services"],
            "currencies": {k: r2(v) for k, v in d["currencies"].items()},
        }

    # AP monthly (top 20 providers per month)
    ap_monthly_out = {}
    for mo in sorted(ap_monthly.keys()):
        ap_monthly_out[mo] = {}
        for p in sorted(ap_monthly[mo].keys(),
                        key=lambda x: ap_monthly[mo][x]["cost"], reverse=True)[:20]:
            d = ap_monthly[mo][p]
            ap_monthly_out[mo][p] = {"cost": r2(d["cost"]), "n": d["n"]}

    # Margin outputs helper
    def margin_out(acc, top_n):
        result = {}
        top = sorted(acc.keys(), key=lambda k: acc[k]["rev"], reverse=True)[:top_n]
        for k in top:
            m = acc[k]
            rev = m["rev"]
            result[k] = {
                "rev": r2(rev), "cost": r2(m["cost"]), "profit": r2(m["profit"]),
                "margin": r2(safe_div(m["profit"], rev) * 100),
                "mkp_avg": r2(safe_div(m["mkp_sum"], m["mkp_n"])),
                "n": m["n"],
            }
        return result

    # Margin monthly trend
    margin_monthly_out = {}
    for mo in sorted(margin_monthly.keys()):
        m = margin_monthly[mo]
        rev = m["rev"]
        margin_monthly_out[mo] = {
            "margin_pct": r2(safe_div(m["profit"], rev) * 100),
            "mkp_avg": r2(safe_div(m["mkp_sum"], m["mkp_n"])),
            "rev": r2(rev), "profit": r2(m["profit"]), "n": m["n"],
        }

    # DSO outputs
    dso_ag_out = {}
    for ag in sorted(dso_agency.keys()):
        s, n = dso_agency[ag]
        if n > 0:
            dso_ag_out[ag] = {"avg_dso": r2(s / n), "n": n}
    dso_monthly_out = {}
    for mo in sorted(dso_monthly_acc.keys()):
        s, n = dso_monthly_acc[mo]
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
                "avg_rate": r2(safe_div(d["rate_sum"], d["rate_n"])),
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
        "invoiced_pct": r2(safe_div(total_inv, total_inv + total_not_inv) * 100),
        "nf_pct": r2(safe_div(total_nf, total_nf + total_nf_pend) * 100),
    }

    # NF by agency (top 50)
    nf_ag_out = {}
    top_nf_ag = sorted(nf_agency.keys(),
                       key=lambda a: nf_agency[a]["with_nf"] + nf_agency[a]["without_nf"],
                       reverse=True)[:TOP_AGENCIES]
    for ag in top_nf_ag:
        d = nf_agency[ag]
        total = d["with_nf"] + d["without_nf"]
        nf_ag_out[ag] = {
            "with_nf": d["with_nf"], "without_nf": d["without_nf"],
            "pct": r2(safe_div(d["with_nf"], total) * 100),
        }

    # YoY output
    yoy_out = {}
    for mo_str, years_data in sorted(yoy_acc.items()):
        yoy_out[mo_str] = {}
        for year_str, ya in sorted(years_data.items()):
            avg_ticket = r2(safe_div(ya["ticket_sum"], ya["ticket_n"]))
            total_for_margin = ya["rev"]
            margin_pct = r2(safe_div(ya["profit"], total_for_margin) * 100)
            yoy_out[mo_str][year_str] = {
                "rev": r2(ya["rev"]),
                "profit": r2(ya["profit"]),
                "margin_pct": margin_pct,
                "n_conf": ya["n_conf"],
                "n_canc": ya["n_canc"],
                "avg_ticket": avg_ticket,
                "room_nights": ya["room_nights"],
                "cost": r2(ya["cost"]),
                "commissions": r2(ya["commissions"]),
                "refunds": r2(ya["refunds"]),
            }

    # Geographic outputs
    geo_country_monthly_out = {}
    for mo in sorted(geo_country_monthly.keys()):
        geo_country_monthly_out[mo] = {
            c: round_dict(v) for c, v in
            sorted(geo_country_monthly[mo].items(), key=lambda x: x[1]["rev"], reverse=True)[:30]
        }

    geo_country_total_out = {}
    for c in sorted(geo_country_total.keys(),
                    key=lambda x: geo_country_total[x]["rev"], reverse=True)[:50]:
        g = geo_country_total[c]
        geo_country_total_out[c] = {
            "rev": r2(g["rev"]), "n": g["n"], "profit": r2(g["profit"]),
            "margin_pct": r2(safe_div(g["profit"], g["rev"]) * 100),
        }

    geo_city_out = {}
    top_cities = sorted(geo_city_acc.keys(),
                        key=lambda x: geo_city_acc[x]["rev"], reverse=True)[:TOP_CITIES]
    for c in top_cities:
        g = geo_city_acc[c]
        geo_city_out[c] = {
            "rev": r2(g["rev"]), "n": g["n"], "profit": r2(g["profit"]),
            "country": g["country"],
        }

    # Room nights monthly
    rn_monthly_out = {}
    for mo in sorted(rn_monthly.keys()):
        rn = rn_monthly[mo]
        avg_stay = r2(safe_div(rn["total_nights"], rn["n_bookings"]))
        rev_per_night = r2(safe_div(
            margin_monthly.get(mo, {}).get("rev", 0), rn["total_nights"]
        )) if rn["total_nights"] > 0 else 0
        rn_monthly_out[mo] = {
            "total_nights": rn["total_nights"],
            "n_bookings": rn["n_bookings"],
            "avg_stay": avg_stay,
            "rev_per_night": rev_per_night,
        }

    # Lead time outputs
    lt_monthly_out = {}
    for mo in sorted(lt_monthly.keys()):
        lt = lt_monthly[mo]
        if lt["n"] > 0:
            lt_monthly_out[mo] = {"avg_days": r2(lt["sum_days"] / lt["n"]), "n": lt["n"]}

    lt_agency_out = {}
    for ag in sorted(lt_agency.keys(), key=lambda a: lt_agency[a]["n"], reverse=True)[:TOP_AGENCIES]:
        lt = lt_agency[ag]
        if lt["n"] > 0:
            lt_agency_out[ag] = {"avg_days": r2(lt["sum_days"] / lt["n"]), "n": lt["n"]}

    # Integration monthly (top 30 per month)
    integ_monthly_out = {}
    for mo in sorted(integ_monthly.keys()):
        integ_monthly_out[mo] = {}
        top_integ = sorted(integ_monthly[mo].keys(),
                           key=lambda x: integ_monthly[mo][x]["rev"], reverse=True)[:30]
        for ig in top_integ:
            integ_monthly_out[mo][ig] = round_dict(integ_monthly[mo][ig])

    integ_total_out = {}
    for ig in sorted(integ_total.keys(), key=lambda x: integ_total[x]["rev"], reverse=True)[:TOP_PROVIDERS]:
        it = integ_total[ig]
        integ_total_out[ig] = {
            "rev": r2(it["rev"]), "n": it["n"], "profit": r2(it["profit"]),
            "margin_pct": r2(safe_div(it["profit"], it["rev"]) * 100),
        }

    # Agency ranking monthly (top 20 per month)
    agency_ranking_out = {}
    for mo in sorted(agency_rank_monthly.keys()):
        ranked = sorted(agency_rank_monthly[mo].items(),
                        key=lambda x: x[1]["rev"], reverse=True)[:20]
        agency_ranking_out[mo] = [
            [ag, r2(v["rev"]), r2(v["profit"]), v["n"],
             r2(safe_div(v["profit"], v["rev"]) * 100)]
            for ag, v in ranked
        ]

    # Daily outputs
    daily_svc_out = {}
    for d in sorted(daily_svc.keys()):
        dd = daily_svc[d]
        dd["avg_ticket"] = r2(safe_div(dd["rev"], dd["n_conf"]))
        dd["margin_pct"] = r2(safe_div(dd["profit"], dd["rev"]) * 100)
        daily_svc_out[d] = round_dict(dd)

    daily_created_out = {}
    for d in sorted(daily_created.keys()):
        dd = daily_created[d]
        dd["avg_ticket"] = r2(safe_div(dd["rev"], dd["n_conf"]))
        dd["margin_pct"] = r2(safe_div(dd["profit"], dd["rev"]) * 100)
        daily_created_out[d] = round_dict(dd)

    # Daily DC (current year only)
    daily_dc_out = {}
    for d in sorted(cf_daily_data.keys()):
        if d.startswith(CURRENT_YEAR):
            daily_dc_out[d] = {
                "credits": r2(cf_daily_data[d][0]),
                "debits": r2(cf_daily_data[d][1]),
                "net": r2(cf_daily_data[d][0] - cf_daily_data[d][1]),
            }

    # Daily top agencies
    daily_top_agencies_out = {}
    for d in sorted(daily_top_agencies_acc.keys()):
        ranked = sorted(daily_top_agencies_acc[d].items(),
                        key=lambda x: x[1]["rev"], reverse=True)[:5]
        daily_top_agencies_out[d] = [[ag, r2(v["rev"]), v["n"]] for ag, v in ranked]

    # Weekly rollup
    weekly_out = {}
    for wk in sorted(weekly_acc.keys()):
        w = weekly_acc[wk]
        weekly_out[wk] = {
            "rev": r2(w["rev"]), "cost": r2(w["cost"]), "profit": r2(w["profit"]),
            "n_conf": w["n_conf"], "n_canc": w["n_canc"],
            "margin_pct": r2(safe_div(w["profit"], w["rev"]) * 100),
            "room_nights": w["room_nights"],
            "week_start": w["week_start"], "week_end": w["week_end"],
        }

    # Tickets resolution monthly
    tkt_resolution_out = {}
    for mo in sorted(tkt_resolution_monthly.keys()):
        tr = tkt_resolution_monthly[mo]
        if tr["n_closed"] > 0:
            tkt_resolution_out[mo] = {
                "avg_hours": r2(tr["sum_hours"] / tr["n_closed"]),
                "n_closed": tr["n_closed"],
            }

    # Tickets fees monthly
    tkt_fees_out = {}
    for mo in sorted(tkt_fees_monthly.keys()):
        tf = tkt_fees_monthly[mo]
        tkt_fees_out[mo] = {"total_fees": r2(tf["total_fees"]), "n": tf["n"]}

    # Tickets by provider (top 30)
    tkt_by_provider_out = dict(sorted(tkt_by_provider.items(),
                                      key=lambda x: x[1], reverse=True)[:30])

    # Determine date range
    all_months = set()
    all_months.update(pnl_monthly.keys())
    all_months.update(cf_monthly.keys())
    all_months.update(inv_monthly.keys())
    sorted_all_months = sorted(all_months) if all_months else []

    # ====================================================================
    # ASSEMBLE FINAL OUTPUT
    # ====================================================================
    output = {
        "metadata": {
            "generated": TODAY,
            "total_services": len(services),
            "total_dc_rows": len(dc_rows),
            "total_tickets": len(tickets),
            "current_year": CURRENT_YEAR,
            "date_range_first": sorted_all_months[0] if sorted_all_months else "",
            "date_range_last": sorted_all_months[-1] if sorted_all_months else "",
        },

        # ===== P&L MONTHLY =====
        "pnl_monthly": {mo: round_dict(pnl_monthly[mo]) for mo in sorted(pnl_monthly.keys())},

        # ===== CASH FLOW =====
        "cashflow_monthly": {mo: round_dict(cf_monthly[mo]) for mo in sorted(cf_monthly.keys())},
        "cashflow_daily": cf_daily_out,

        # ===== REVENUE RECOGNITION =====
        "rev_by_service_date": {mo: round_dict(rev_svc_date[mo]) for mo in sorted(rev_svc_date.keys())},
        "rev_by_created_date": {mo: round_dict(rev_created[mo]) for mo in sorted(rev_created.keys())},
        "rev_by_finan_date": {mo: round_dict(rev_finan[mo]) for mo in sorted(rev_finan.keys())},

        # ===== AR / AP =====
        "ar_by_agency": {ag: round_dict(ar_agency[ag])
                         for ag in sorted(ar_agency.keys(),
                                          key=lambda a: ar_agency[a]["not_invoiced"],
                                          reverse=True)[:TOP_AGENCIES]},
        "ar_aging": ar_aging_out,
        "ar_aging_by_agency": ar_aging_ag_out,
        "ap_by_provider": ap_out,
        "ap_monthly": ap_monthly_out,

        # ===== MARGIN =====
        "margin_monthly_trend": margin_monthly_out,
        "margin_by_agency": margin_out(margin_agency, TOP_AGENCIES),
        "margin_by_provider": margin_out(margin_provider, TOP_PROVIDERS),
        "margin_by_hotel_top100": margin_out(margin_hotel, TOP_HOTELS),
        "negative_margin_services": neg_margin[:200],

        # ===== CANCELLATION =====
        "cancellation_monthly": {mo: round_dict(canc_monthly[mo]) for mo in sorted(canc_monthly.keys())},
        "cancellation_by_agency": {ag: round_dict(canc_agency[ag])
                                   for ag in sorted(canc_agency.keys(),
                                                    key=lambda a: canc_agency[a]["lost_rev"],
                                                    reverse=True)[:TOP_AGENCIES]},

        # ===== CURRENCY / PAYMENT / SOURCE =====
        "currency_monthly": curr_monthly_out,
        "payment_mix_monthly": {mo: {pt: round_dict(v) for pt, v in d.items()}
                                for mo, d in sorted(pay_monthly.items())},
        "source_mix_monthly": {mo: {s: round_dict(v) for s, v in d.items()}
                               for mo, d in sorted(src_monthly.items())},

        # ===== COMMISSIONS =====
        "commission_monthly": {mo: round_dict(comm_monthly[mo]) for mo in sorted(comm_monthly.keys())},
        "commission_by_agency": {ag: round_dict(comm_agency[ag])
                                 for ag in sorted(comm_agency.keys(),
                                                  key=lambda a: comm_agency[a]["total"],
                                                  reverse=True)[:TOP_AGENCIES]},

        # ===== REFUNDS =====
        "refund_monthly": {mo: round_dict(ref_monthly[mo]) for mo in sorted(ref_monthly.keys())},
        "refund_by_agency": {ag: round_dict(ref_agency[ag])
                             for ag in sorted(ref_agency.keys(),
                                              key=lambda a: ref_agency[a]["ref_c"],
                                              reverse=True)[:TOP_AGENCIES]},
        "refund_by_provider": {p: round_dict(ref_provider[p])
                               for p in sorted(ref_provider.keys(),
                                               key=lambda x: ref_provider[x]["ref_c"],
                                               reverse=True)[:TOP_PROVIDERS]},

        # ===== DISPUTES =====
        "disputes_monthly": {mo: disp_monthly[mo] for mo in sorted(disp_monthly.keys())},
        "disputes_by_agency": dict(sorted(disp_agency.items(), key=lambda x: x[1], reverse=True)[:TOP_AGENCIES]),
        "disputes_by_hotel": dict(sorted(disp_hotel.items(), key=lambda x: x[1], reverse=True)[:TOP_HOTELS]),

        # ===== INVOICE / NF / DSO =====
        "invoice_summary": invoice_summary,
        "invoice_monthly": {mo: round_dict(inv_monthly[mo]) for mo in sorted(inv_monthly.keys())},
        "nf_by_agency": nf_ag_out,
        "dso_by_agency": dso_ag_out,
        "dso_monthly": dso_monthly_out,

        # ===== UPCOMING =====
        "upcoming_checkins": {mo: round_dict(upcoming[mo]) for mo in sorted(upcoming.keys())[:6]},

        # ===== TICKETS =====
        "tickets_monthly": dict(sorted(tkt_monthly_count.items())),

        # ===== P&L BY ENTITY =====
        "pnl_by_agency": {mo: {ag: round_dict(v) for ag, v in
                                sorted(data.items(), key=lambda x: x[1]["rev"], reverse=True)[:30]}
                          for mo, data in sorted(pnl_agency.items())},
        "pnl_by_provider": {mo: {p: round_dict(v) for p, v in
                                  sorted(data.items(), key=lambda x: x[1]["rev"], reverse=True)[:20]}
                            for mo, data in sorted(pnl_provider.items())},

        # ===== YOY COMPARISON =====
        "yoy": yoy_out,

        # ===== GEOGRAPHIC =====
        "geo_country_monthly": geo_country_monthly_out,
        "geo_country_total": geo_country_total_out,
        "geo_city_top50": geo_city_out,

        # ===== ROOM NIGHTS =====
        "room_nights_monthly": rn_monthly_out,

        # ===== LEAD TIME =====
        "lead_time_monthly": lt_monthly_out,
        "lead_time_by_agency": lt_agency_out,

        # ===== INTEGRATION / CHANNEL =====
        "integration_monthly": integ_monthly_out,
        "integration_total": integ_total_out,

        # ===== AGENCY RANKING =====
        "agency_ranking_monthly": agency_ranking_out,

        # ===== DAILY DATA (current year, enriched) =====
        "daily_by_service_date": daily_svc_out,
        "daily_by_created_date": daily_created_out,
        "daily_dc": daily_dc_out,
        "daily_top_agencies": daily_top_agencies_out,

        # ===== WEEKLY ROLLUP (current year) =====
        "weekly_current_year": weekly_out,

        # ===== TICKETS DEEP =====
        "tickets_by_priority": dict(tkt_by_priority),
        "tickets_by_department": dict(sorted(tkt_by_department.items(), key=lambda x: x[1], reverse=True)),
        "tickets_by_status": dict(tkt_by_status),
        "tickets_by_type": dict(sorted(tkt_by_type.items(), key=lambda x: x[1], reverse=True)),
        "tickets_resolution_monthly": tkt_resolution_out,
        "tickets_fees_monthly": tkt_fees_out,
        "tickets_by_provider": tkt_by_provider_out,

        # ===== PROTECTION / REFUNDABLE =====
        "protection_monthly": {mo: round_dict(prot_monthly[mo]) for mo in sorted(prot_monthly.keys())},
        "refundable_monthly": {mo: refundable_monthly[mo] for mo in sorted(refundable_monthly.keys())},

        # ===== BOOKING VALUE DISTRIBUTION =====
        "booking_value_distribution": {
            "buckets": BUCKET_LABELS,
            "counts": bvd_counts,
            "revenue": [r2(v) for v in bvd_revenue],
        },

        # ===== PAYMENT DEADLINE COMPLIANCE =====
        "payment_deadline_monthly": {mo: pay_deadline[mo] for mo in sorted(pay_deadline.keys())},

        "current_year": CURRENT_YEAR,
    }

    # ====================================================================
    # AI INSIGHTS
    # ====================================================================
    print("\n  Generating AI Insights...")
    insights = generate_insights(output)
    output["ai_insights"] = insights
    print(f"  Generated {len(insights)} insights")

    # ====================================================================
    # INJECT INTO HTML
    # ====================================================================
    print("\n  Injecting into Dashboard_Financeiro.html...")

    json_str = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
    json_mb = len(json_str) / 1024 / 1024
    print(f"  JSON size: {len(json_str):,} bytes ({json_mb:.1f} MB)")

    if not os.path.exists(HTML_FILE):
        fallback = os.path.join(BASE_DIR, "dashboard_data.json")
        print(f"  WARNING: {HTML_FILE} not found")
        print(f"  Writing JSON to {fallback}")
        with open(fallback, "w", encoding="utf-8") as f:
            f.write(json_str)
        print("  Done! Create Dashboard_Financeiro.html and run again.")
        return output

    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    marker_start = '<script id="dashData" type="application/json">'
    marker_end = "</script>"
    idx_start = html.find(marker_start)
    if idx_start == -1:
        fallback = os.path.join(BASE_DIR, "dashboard_data.json")
        print(f"  ERROR: marker <script id='dashData'> not found in HTML!")
        print(f"  Writing JSON to {fallback}")
        with open(fallback, "w", encoding="utf-8") as f:
            f.write(json_str)
        return output

    idx_content_start = idx_start + len(marker_start)
    idx_end = html.find(marker_end, idx_content_start)

    new_html = html[:idx_content_start] + "\n" + json_str + "\n" + html[idx_end:]

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(new_html)

    print(f"  Dashboard updated: {HTML_FILE}")

    # ====================================================================
    # SUMMARY
    # ====================================================================
    print("\n" + "=" * 70)
    print("  BUILD COMPLETE")
    print("=" * 70)
    print(f"  Services:   {len(services):>8,}")
    print(f"  DC Rows:    {len(dc_rows):>8,}")
    print(f"  Tickets:    {len(tickets):>8,}")
    print(f"  Months:     {len(sorted_all_months):>8}")
    print(f"  JSON Keys:  {len(output):>8}")
    print(f"  Insights:   {len(insights):>8}")
    print(f"  JSON Size:  {json_mb:>7.1f} MB")
    print("=" * 70)

    return output


if __name__ == "__main__":
    build()
