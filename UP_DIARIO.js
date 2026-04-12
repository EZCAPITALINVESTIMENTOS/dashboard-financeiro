/**
 * CR FINANCIAL EZLINK — UP_DIARIO: extração de range com 4 datasets de uma vez
 *
 * Extrai para o range configurado abaixo:
 *   1. Services por CHECK-IN    → CR_Services_YYYY-MM.csv           (um por mês)
 *   2. Services por CRIAÇÃO     → CR_Services_Created_YYYY-MM.csv   (um por mês)
 *   3. Tickets                  → CR_Tickets_YYYY-MM.csv            (um por mês)
 *   4. DebitCredit              → CR_DebitCredit_range_FROM_TO.csv  (arquivo único)
 *
 * COMO USAR:
 *   1. Ajuste DATE_FROM e DATE_TO logo abaixo
 *   2. Abra https://ezlink.global/customer/#/services/general (logado)
 *   3. F12 → Console → cole este arquivo inteiro → Enter
 *   4. Aguarde todos os CSVs baixarem em ~/Downloads
 *   5. Mova para as pastas em ~/iCloud/CR FINANCIAL EZLINK/:
 *        CR_Services_*.csv            → Services/
 *        CR_Services_Created_*.csv    → Services_Created/
 *        CR_Tickets_*.csv             → Tickets/
 *        CR_DebitCredit_range_*.csv   → DebitCredit/
 *   6. Rode: python3 build_financial_data.py
 *
 * NOTA: build_financial_data.py faz dedup por `id`, então append-only é seguro —
 *       rodar o script por um range que sobrepõe dados existentes não duplica.
 */
(async function() {
  // ==========================================
  //   CONFIGURE AQUI O RANGE DE DATAS
  // ==========================================
  const DATE_FROM = '2026-04-01';
  const DATE_TO   = '2026-04-12';
  // ==========================================

  console.log(`%c=== CR FINANCIAL EZLINK • UP_DIARIO (${DATE_FROM} → ${DATE_TO}) ===`,
              'background:#1A3A5C;color:#F5A623;padding:4px 8px;font-weight:bold');

  const store = document.querySelector('#app').__vue__.$store;
  const token = store.state.config.token.access_token;
  const apiURL = store.state.config.apiURL;
  if (!token) { console.error('Nao esta logado!'); return; }

  const headers = {
    'Authorization': 'Bearer ' + token,
    'Version': '7.18',
    'Accept': 'application/json, text/plain, */*',
    'X-Requested-With': 'XMLHttpRequest'
  };

  // -- HELPERS --
  const sleep   = ms => new Promise(r => setTimeout(r, ms));
  const lastDay = mo => { const [y,m] = mo.split('-').map(Number); return new Date(y, m, 0).getDate(); };

  function getMonthsInRange(from, to) {
    const months = [];
    let [y, m]     = from.split('-').map(Number);
    const [yE, mE] = to.split('-').map(Number);
    while (y < yE || (y === yE && m <= mE)) {
      months.push(`${y}-${String(m).padStart(2, '0')}`);
      m++;
      if (m > 12) { m = 1; y++; }
    }
    return months;
  }

  async function fetchPage(url, label) {
    for (let attempt = 1; attempt <= 6; attempt++) {
      try {
        const freshToken = document.querySelector('#app').__vue__.$store.state.config.token.access_token;
        if (!freshToken) throw new Error('token ausente (sessao expirou?)');
        const freshHeaders = { ...headers, Authorization: 'Bearer ' + freshToken };
        const resp = await fetch(url, { headers: freshHeaders, cache: 'no-store' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status} ${resp.statusText}`);
        const text = await resp.text();
        if (!text || text.trim().length === 0) throw new Error('body vazio (0 bytes)');
        try {
          return JSON.parse(text);
        } catch (parseErr) {
          throw new Error(`JSON invalido (${text.length} bytes): "${text.slice(0, 80)}"`);
        }
      } catch (e) {
        if (attempt === 6) {
          console.error(`      ${label}: falhou apos 6 tentativas �� ${e.message}`);
          return null;
        }
        console.warn(`      ${label}: tentativa ${attempt}/6 falhou (${e.message}). Aguardando 90s...`);
        await sleep(90000);
      }
    }
  }

  function downloadCSV(data, filename, delimiter = ';') {
    if (!data || data.length === 0) {
      console.log(`      ${filename}: sem dados, nao baixou`);
      return 0;
    }
    const allKeys = new Set();
    data.forEach(row => Object.keys(row).forEach(k => allKeys.add(k)));
    const cols = Array.from(allKeys).sort();
    let csv = '\uFEFF' + cols.join(delimiter) + '\n';
    data.forEach(row => {
      const line = cols.map(h => {
        let val = row[h];
        if (val === null || val === undefined) return '';
        if (typeof val === 'object') val = JSON.stringify(val);
        val = String(val);
        if (val.includes(delimiter) || val.includes('"') || val.includes('\n')) {
          val = '"' + val.replace(/"/g, '""') + '"';
        }
        return val;
      }).join(delimiter);
      csv += line + '\n';
    });
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
    return data.length;
  }

  async function fetchServicesRange(paramStart, paramEnd, from, to, label) {
    let data = [];
    let page = 1;
    let totalAPI = 0;
    const PER_PAGE = 1000;
    while (true) {
      const url = `${apiURL}/services?page=${page}&per_page=${PER_PAGE}&view=general&sort=id%7Cdesc&user_id=10&agency_id=0&assignment=0&${paramStart}=${from}&${paramEnd}=${to}`;
      const json = await fetchPage(url, `${label} pag ${page}`);
      if (!json) break;
      if (page === 1) {
        totalAPI = json.total || 0;
        console.log(`      Total API: ${totalAPI} / ${json.last_page || 1} pgs x ${PER_PAGE}`);
      }
      if (!json.data || json.data.length === 0) break;
      data = data.concat(json.data);
      console.log(`      Pag ${page}/${json.last_page || 1} — ${data.length}/${totalAPI}`);
      if (page >= (json.last_page || 1)) break;
      page++;
    }
    return { data, totalAPI };
  }

  const MONTHS = getMonthsInRange(DATE_FROM, DATE_TO);
  console.log(`Meses cobertos: ${MONTHS.join(', ')} (${MONTHS.length} mes${MONTHS.length > 1 ? 'es' : ''})`);

  const summary = MONTHS.map(mo => ({ mes: mo, checkin: '—', criacao: '—', tickets: '—' }));
  const findRow = mo => summary.find(s => s.mes === mo);

  // -- PASSO 1: SERVICES (por mes, 2 datasets) --
  for (let i = 0; i < MONTHS.length; i++) {
    const mo = MONTHS[i];
    const monthStart = `${mo}-01`;
    const monthEnd   = `${mo}-${String(lastDay(mo)).padStart(2,'0')}`;
    const from = (monthStart < DATE_FROM) ? DATE_FROM : monthStart;
    const to   = (monthEnd   > DATE_TO)   ? DATE_TO   : monthEnd;

    console.log(`\n${'─'.repeat(60)}`);
    console.log(`[${i+1}/${MONTHS.length}] ${mo}  (${from} → ${to})`);

    console.log(`   Check-in (checkIn/checkInTo)...`);
    const ci = await fetchServicesRange('checkIn', 'checkInTo', from, to, `${mo} check-in`);
    const ciName = `CR_Services_${mo}.csv`;
    const ciN = downloadCSV(ci.data, ciName);
    console.log(`      ${ciName}: ${ciN}/${ci.totalAPI}`);
    findRow(mo).checkin = ciN;
    await sleep(500);

    console.log(`   Criacao (serviceDate/serviceDateEnd)...`);
    const cr = await fetchServicesRange('serviceDate', 'serviceDateEnd', from, to, `${mo} criacao`);
    const crName = `CR_Services_Created_${mo}.csv`;
    const crN = downloadCSV(cr.data, crName);
    console.log(`      ${crName}: ${crN}/${cr.totalAPI}`);
    findRow(mo).criacao = crN;
    await sleep(500);
  }

  // -- PASSO 2: TICKETS (passe unico sort=id|desc, bucket por mes) --
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Tickets (passe unico, para em created_at < ${DATE_FROM})`);
  let tickets = [];
  {
    let page = 1;
    let done = false;
    const PER_PAGE = 500;
    const MAX_PAGES = 500;
    while (!done && page <= MAX_PAGES) {
      const url = `${apiURL}/occurrences_customer?page=${page}&per_page=${PER_PAGE}&sort=id%7Cdesc&view=general&user_id=0`;
      const json = await fetchPage(url, `tickets pag ${page}`);
      if (!json || !json.data || json.data.length === 0) break;
      let kept = 0, skippedFuture = 0;
      for (const t of json.data) {
        const created = (t.created_at || '').slice(0, 10);
        if (!created) continue;
        if (created >= DATE_FROM && created <= DATE_TO) {
          tickets.push(t);
          kept++;
        } else if (created < DATE_FROM) {
          done = true;
          break;
        } else {
          skippedFuture++;
        }
      }
      console.log(`   Pag ${page}/${json.last_page || '?'}: +${kept} (pulou futuro ${skippedFuture}) -> total ${tickets.length}`);
      if (page >= (json.last_page || 1)) break;
      page++;
    }
  }
  const ticketsByMonth = {};
  for (const t of tickets) {
    const mo = (t.created_at || '').slice(0, 7);
    if (!mo) continue;
    (ticketsByMonth[mo] = ticketsByMonth[mo] || []).push(t);
  }
  for (const mo of Object.keys(ticketsByMonth).sort()) {
    const arr = ticketsByMonth[mo];
    const tName = `CR_Tickets_${mo}.csv`;
    const n = downloadCSV(arr, tName);
    console.log(`   ${tName}: ${n}`);
    if (findRow(mo)) findRow(mo).tickets = n;
  }
  console.log(`   Tickets total no range: ${tickets.length}`);

  // -- PASSO 3: DEBIT/CREDIT (passe unico, arquivo unico) --
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`DebitCredit (passe unico, para em finan_date < ${DATE_FROM})`);
  let dc = [];
  {
    let page = 1;
    let done = false;
    const PER_PAGE = 500;
    const MAX_PAGES = 500;
    while (!done && page <= MAX_PAGES) {
      const url = `${apiURL}/finances/debit_credit?page=${page}&per_page=${PER_PAGE}&sort=finan_date%7Cdesc&search=&status=0`;
      const json = await fetchPage(url, `debit_credit pag ${page}`);
      if (!json || !json.data || json.data.length === 0) break;
      let kept = 0, skippedFuture = 0;
      for (const d of json.data) {
        const fdate = (d.finan_date || '').slice(0, 10);
        if (!fdate) continue;
        if (fdate >= DATE_FROM && fdate <= DATE_TO) {
          dc.push(d);
          kept++;
        } else if (fdate < DATE_FROM) {
          done = true;
          break;
        } else {
          skippedFuture++;
        }
      }
      console.log(`   Pag ${page}/${json.last_page || '?'}: +${kept} (pulou futuro ${skippedFuture}) -> total ${dc.length}`);
      if (page >= (json.last_page || 1)) break;
      page++;
    }
  }
  const dcName = `CR_DebitCredit_range_${DATE_FROM}_${DATE_TO}.csv`;
  const dcN = downloadCSV(dc, dcName, ',');
  console.log(`   ${dcName}: ${dcN}`);

  // -- SUMMARY --
  console.log('\n' + '='.repeat(60));
  console.log(`%cUP_DIARIO FINANCIAL COMPLETO (${DATE_FROM} → ${DATE_TO})`,
              'background:#F5A623;color:#1A3A5C;padding:4px 8px;font-weight:bold');
  console.table(summary);
  console.log(`Tickets total: ${tickets.length}`);
  console.log(`DebitCredit:   ${dcN}`);
  console.log(`\nPROXIMOS PASSOS:`);
  console.log(`   1. Mova os arquivos em ~/Downloads pras pastas certas:`);
  console.log(`        CR_Services_*.csv           -> ~/iCloud/CR FINANCIAL EZLINK/Services/`);
  console.log(`        CR_Services_Created_*.csv   -> ~/iCloud/CR FINANCIAL EZLINK/Services_Created/`);
  console.log(`        CR_Tickets_*.csv            -> ~/iCloud/CR FINANCIAL EZLINK/Tickets/`);
  console.log(`        CR_DebitCredit_range_*.csv  -> ~/iCloud/CR FINANCIAL EZLINK/DebitCredit/`);
  console.log(`   2. Rode: python3 build_financial_data.py`);
  console.log('='.repeat(60));
})();
