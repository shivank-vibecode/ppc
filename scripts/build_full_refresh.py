"""
Full refresh of cohort-driven dashboards in PPC_Geo_Individual.html
from the latest April 26 CSVs.

Rebuilds in place:
  - COHORT_DATA  (Cohort Speed view)
  - COHORT_ROAS  (Cohort ROAS view)
  - SCALING_DATA (Scaling Decisions view)

All-platforms (Google + Bing + FB + MQL5 + organic-attributed) per the
user's standing requirement; only campaigns with a non-empty Campaign
Name are included.
"""

import csv, json, re, statistics, pathlib
from datetime import datetime, date, timedelta
from collections import defaultdict, Counter

# Resolve paths relative to the project root (the parent of this script's
# directory). This lets the same script run unchanged in Cowork, Cursor, or
# any user's local clone — no hard-coded /sessions/... paths.
import glob as _glob, os as _os, pathlib as _pl
_THIS_DIR = _pl.Path(__file__).resolve().parent
_PROJECT_ROOT = _THIS_DIR.parent
_cohort_files = sorted(_glob.glob(str(_PROJECT_ROOT / 'cohort' / '*.csv')), key=_os.path.getmtime)
_nc_files     = sorted(_glob.glob(str(_PROJECT_ROOT / 'Non cohort' / '*.csv')), key=_os.path.getmtime)
COHORT_CSV = _cohort_files[-1] if _cohort_files else None
NC_CSV     = _nc_files[-1]     if _nc_files     else None
HTML_PATH  = str(_PROJECT_ROOT / 'PPC_Geo_Individual.html')
print(f"Project root:      {_PROJECT_ROOT}")
print(f"Latest cohort CSV: {COHORT_CSV}")
print(f"Latest NC CSV:     {NC_CSV}")

# ── Dynamic date configuration ────────────────────────────────────────
# SNAPSHOT is auto-detected from the cohort CSV's latest joined_date,
# or can be overridden manually. Everything else derives from it.
import calendar

def _detect_snapshot():
    """Scan the cohort CSV for the latest joined_date to infer snapshot."""
    latest = None
    with open(COHORT_CSV, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            s = (row.get('joined_date') or '').strip()
            if not s:
                continue
            d = None
            for fmt in ("%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
                try:
                    d = datetime.strptime(s, fmt).date()
                    break
                except ValueError:
                    pass
            if d and (latest is None or d > latest):
                latest = d
    return latest or date.today()

SNAPSHOT     = _detect_snapshot()
WINDOW_START = date(SNAPSHOT.year - 1, 1, 1) if SNAPSHOT.month == 1 else date(SNAPSHOT.year, 1, 1)
WINDOW_END   = SNAPSHOT

# The "target month" is the month of the snapshot — the month we're tracking.
# The "baseline period" is everything before it in the same year.
TARGET_MONTH       = SNAPSHOT.month
TARGET_YEAR        = SNAPSHOT.year
DAYS_IN_TARGET_MONTH = calendar.monthrange(TARGET_YEAR, TARGET_MONTH)[1]

# Monthly NC funded target — the business goal for the target month.
# Adjust this when targets change across months.
NC_MONTHLY_TARGET  = 3000

# ── geo classification (mirrors build_comparison.py) ─────────────────
GEO_RULES = [
    ('Nigeria',       lambda c: c.startswith('ng-') or c.startswith('ng_')),
    ('Africa Tier 1', lambda c: 'africa_tier1' in c[:22]),
    ('Africa Tier 2', lambda c: 'africa_tier2' in c[:22]),
    ('Africa Tier 3', lambda c: 'africa_tier3' in c[:22] or c.startswith('aafrica_tier3')),
    ('Africa Tier 4', lambda c: 'africa_tier4' in c[:22]),
    ('Africa Tier 5', lambda c: 'africa_tier5' in c[:22]),
    ('Asia Tier 1',   lambda c: 'asia_tier1' in c[:20]),
    ('Asia Tier 2',   lambda c: 'asia_tier2' in c[:20]),
    ('LATAM',         lambda c: c.startswith('latam')),
    ('Jamaica',       lambda c: c.startswith('jm-') or c.startswith('jm_')),
    ('Middle East',   lambda c: c.startswith('middle_east') or c.startswith('mena_mideast')),
    ('CIS',           lambda c: c.startswith('cis')),
    # ROW2 / ROW1 must be checked BEFORE bare ROW so they don't get swallowed
    ('ROW2',          lambda c: c.startswith('row2-') or c.startswith('row2_')),
    ('ROW1',          lambda c: c.startswith('row1-') or c.startswith('row1_')),
    ('ROW',           lambda c: c.startswith('row-')  or c.startswith('row_')),
    ('EU',            lambda c: c.startswith('eu-')   or c.startswith('eu_')),
]

PIC_MAP = {
    'Conor Horan':  ['Nigeria', 'Africa Tier 1', 'Africa Tier 2', 'Africa Tier 3', 'CIS', 'ROW2'],
    'Ishank Naik':  ['Jamaica', 'Asia Tier 1', 'Africa Tier 5', 'Middle East', 'EU', 'ROW1'],
    'Aswathi Unni': ['LATAM', 'Asia Tier 2', 'Africa Tier 4', 'ROW', 'Other'],
}
GEO_TO_PIC = {g: p for p, gs in PIC_MAP.items() for g in gs}

# Per-campaign PIC overrides — takes precedence over geo-based PIC mapping.
# Use for campaigns whose name doesn't match a tier prefix but is genuinely
# owned by a specific PIC. Add new ones here as ownership decisions land.
CAMPAIGN_PIC_OVERRIDES = {
    'africa-prosp-cpa-all-brand_keyword-0222-en-bing': 'Conor Horan',
    'africa-prosp-cpa-all-deriv_dmt5-0222-en-bing':    'Conor Horan',
    'africa-prosp-cpa-all-fx_spread-0222-en-bing':     'Conor Horan',
    'asia-lk-prosp-cpc-all-brand_keyword-1023-en-bing': 'Ishank Naik',
}

def pic_for(campaign, geo):
    """Resolve PIC for a campaign — overrides take precedence over geo map."""
    if campaign in CAMPAIGN_PIC_OVERRIDES:
        return CAMPAIGN_PIC_OVERRIDES[campaign]
    return GEO_TO_PIC.get(geo, 'Unassigned')

# Campaigns whose name starts with any of these prefixes (case-insensitive)
# are dropped from the entire dashboard. Sub-region buckets that aren't
# managed by any specific PIC and aren't the canonical tier classification.
EXCLUDED_PREFIXES = (
    'africa_east', 'africa_south', 'africa_west', 'africa_french', 'africa_port',
    'asia_isc', 'asia_sea',
    'mena_mideast', 'mena_africa',
)
# Suffixes that mark campaigns present in the cohort CSV but absent from
# the NC CSV (demand-gen pipelines that don't carry daily spend/lead rows).
# Excluding them keeps cohort counts aligned with NC counts.
EXCLUDED_SUFFIXES = ('-dgen',)

def is_excluded(campaign):
    cl = (campaign or '').lower()
    if cl.startswith(EXCLUDED_PREFIXES):
        return True
    if cl.endswith(EXCLUDED_SUFFIXES):
        return True
    return False
EXP_KW = ['experiment', 'test', 'ab_test', 'permax']


def classify_geo(c):
    """ROW is now strictly defined as campaigns with 'row' in the name
    (handled by the row-/row1-/row2- prefix rules above). Anything else
    that doesn't match a specific tier falls into 'Other', which the
    PIC map assigns to Aswathi to keep the Unassigned bucket empty."""
    if not c: return 'Other'
    cl = c.lower()
    for g, rule in GEO_RULES:
        if rule(cl): return g
    return 'Other'


def classify_type(c):
    cl = (c or '').lower()
    return 'Exp' if any(kw in cl for kw in EXP_KW) else 'Core'


def parse_date(s):
    if not s: return None
    s = s.strip()
    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try: return datetime.strptime(s, fmt).date()
        except ValueError: pass
    return None


def month_key(d):
    return f"{d.year:04d}-{d.month:02d}"


def month_label(key):
    y, m = key.split('-')
    return datetime(int(y), int(m), 1).strftime("%b %Y")


def week_start(d):                      # Monday-based ISO week start
    return d - timedelta(days=d.weekday())


def week_label(d):
    s = week_start(d)
    e = s + timedelta(days=6)
    return f"{s.strftime('%b %d')}\u2013{e.strftime('%b %d')}"


def pctile(arr, p):
    if not arr: return 0.0
    arr = sorted(arr)
    k = (len(arr) - 1) * (p / 100)
    f, c = int(k), min(int(k)+1, len(arr)-1)
    return arr[f] + (arr[c]-arr[f]) * (k-f)


# ── 1. Load cohort CSV ────────────────────────────────────────────────
print("Loading cohort CSV…")
users = []
excluded_users = 0
with open(COHORT_CSV, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        camp = (row.get('Campaign Name') or '').strip()
        if not camp: continue
        if is_excluded(camp):
            excluded_users += 1
            continue
        joined = parse_date(row.get('joined_date'))
        if not joined or joined < WINDOW_START or joined > WINDOW_END: continue
        ftd = parse_date(row.get('first_deposit_date'))
        try: dep = float(row.get('lifetime_deposit_usd') or 0)
        except: dep = 0.0
        try: pnl = float(row.get('lifetime_company_pnl_usd') or 0)
        except: pnl = 0.0
        tt = (ftd - joined).days if ftd else None
        users.append({
            'campaign': camp,
            'geo':      classify_geo(camp),
            'type':     classify_type(camp),
            'joined':   joined,
            'ftd':      ftd,
            'tt_fund':  tt,
            'deposit':  dep,
            'pnl':      pnl,
        })
print(f"  {len(users):,} users in window  (excluded {excluded_users:,} sub-region rows)")


# ── 2. Load non-cohort CSV ───────────────────────────────────────────
# The CSV format alternates: sometimes rolled up per (Platform, Campaign)
# without a Date column, sometimes with daily Date breakdown. Handle both.
# When dates ARE present, also collect rich per-row data so we can rebuild
# the Overall Funnel m_funnel / w_funnel / daily_funnel directly.
print("Loading non-cohort CSV…")
nc_spend   = defaultdict(float)
nc_revenue = defaultdict(float)
nc_funded  = defaultdict(int)
nc_demo    = defaultdict(int)
nc_real    = defaultdict(int)
nc_funded_by_month = defaultdict(int)
nc_total_spend = 0.0
nc_total_funded = 0
nc_has_dates = False
nc_rows = []                  # per-row when dated — fuels Overall Funnel rebuild

def _is_exp_campaign(name, campaign_type):
    n = (name or '').lower()
    if any(kw in n for kw in EXP_KW): return True
    t = (campaign_type or '').lower()
    if 'experiment' in t or 'permax' in t or 'test' in t: return True
    return False

def _is_brand_kw(name):
    n = (name or '').lower()
    return ('brand_keyword' in n) and ('tagline' not in n)


excluded_nc_rows = 0
with open(NC_CSV, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        camp = (row.get('Campaign') or '').strip()
        if not camp: continue
        if is_excluded(camp):
            excluded_nc_rows += 1
            continue
        try: sp = float(row.get('Spend') or 0)
        except: sp = 0.0
        try: rv = float(row.get('Revenue') or 0)
        except: rv = 0.0
        try: fn = int(float(row.get('New Funded') or 0))
        except: fn = 0
        try: lds = int(float(row.get('Leads') or row.get('V2 Leads') or 0))
        except: lds = 0
        try: rl  = int(float(row.get('Real') or 0))
        except: rl = 0
        nc_spend[camp]   += sp
        nc_revenue[camp] += rv
        nc_funded[camp]  += fn
        nc_demo[camp]    += lds
        nc_real[camp]    += rl
        nc_total_spend  += sp
        nc_total_funded += fn
        d = parse_date(row.get('Date'))
        if d:
            nc_has_dates = True
            nc_funded_by_month[month_key(d)] += fn
            nc_rows.append({
                'date':    d,
                'campaign':camp,
                'type':    row.get('Campaign Type'),
                'is_exp':  _is_exp_campaign(camp, row.get('Campaign Type')),
                'is_brand':_is_brand_kw(camp),
                'demo':    lds,
                'real':    rl,
                'funded':  fn,
                'spend':   sp,
                'revenue': rv,
                'geo':     classify_geo(camp),
            })
print(f"  {len(nc_spend):,} campaigns, total spend ${nc_total_spend:,.0f}, NC funded {nc_total_funded:,}  (excluded {excluded_nc_rows:,} sub-region rows)")
if nc_has_dates:
    print(f"  NC CSV has Date column — using direct per-month NC funded counts")
    for mk in sorted(nc_funded_by_month.keys()):
        print(f"    {mk}: {nc_funded_by_month[mk]:,}")
    print(f"  Captured {len(nc_rows):,} dated rows for Overall Funnel rebuild")


# ── 3. Build COHORT_DATA (Cohort Speed view) ─────────────────────────
print("\nBuilding COHORT_DATA…")
def bucket_dist(tt_list):
    """Return (count, p25, median, p75, avg, same_day, w3, w7, w14, w30, over30) for a list of tt_fund days."""
    n = len(tt_list)
    if not n:
        return dict(funded=0, avg_days=0, median_days=0, p25_days=0, p75_days=0,
                    same_day=0, within_3d=0, within_7d=0, within_14d=0, within_30d=0, over_30d=0)
    return dict(
        funded=n,
        avg_days=round(sum(tt_list)/n, 1),
        median_days=round(statistics.median(tt_list), 1),
        p25_days=round(pctile(tt_list, 25), 1),
        p75_days=round(pctile(tt_list, 75), 1),
        same_day=sum(1 for x in tt_list if x == 0),
        within_3d=sum(1 for x in tt_list if x <= 3),
        within_7d=sum(1 for x in tt_list if x <= 7),
        within_14d=sum(1 for x in tt_list if x <= 14),
        within_30d=sum(1 for x in tt_list if x <= 30),
        over_30d=sum(1 for x in tt_list if x > 30),
    )


def build_period_cohorts(period_key_fn, period_label_fn, signup_keyer):
    """Return list of dicts and a per-period campaign drill mapping."""
    bucket_users = defaultdict(list)
    bucket_funded_users = defaultdict(list)
    bucket_signups = defaultdict(int)
    bucket_deposit = defaultdict(float)
    drill_users = defaultdict(lambda: defaultdict(list))   # bucket -> campaign -> list of users
    for u in users:
        b = period_key_fn(u['joined'])
        bucket_signups[b] += 1
        if u['ftd'] and u['tt_fund'] is not None:
            bucket_funded_users[b].append(u['tt_fund'])
            bucket_deposit[b] += u['deposit']
            drill_users[b][u['campaign']].append(u)
    rows = []
    for b in sorted(bucket_signups.keys()):
        tt = bucket_funded_users[b]
        d = bucket_dist(tt)
        d['signups'] = bucket_signups[b]
        d['total_deposit'] = round(bucket_deposit[b], 2)
        d['label'] = period_label_fn(b)
        rows.append((b, d))
    return rows, drill_users


def render_drill(drill_users):
    """Convert {bucket: {camp: [users]}} → {bucket: [campaign_dict]}."""
    out = {}
    for bucket, camps in drill_users.items():
        rows = []
        for camp, ulist in camps.items():
            tt = [u['tt_fund'] for u in ulist if u['tt_fund'] is not None]
            d = bucket_dist(tt)
            rows.append({
                'campaign': camp,
                'geo':      classify_geo(camp),
                'type':     classify_type(camp),
                'funded':   d['funded'],
                'avg_days': d['avg_days'],
                'median_days': d['median_days'],
                'same_day': d['same_day'],
                'within_7d':d['within_7d'],
                'within_14d':d['within_14d'],
                'within_30d':d['within_30d'],
                'over_30d': d['over_30d'],
                'total_deposit': round(sum(u['deposit'] for u in ulist), 2),
            })
        rows.sort(key=lambda r: -r['funded'])
        out[bucket] = rows
    return out


# Daily
daily_rows, daily_drill_users = build_period_cohorts(
    lambda d: d.isoformat(),
    lambda k: datetime.strptime(k, '%Y-%m-%d').strftime('%b %-d'),
    lambda u: u['joined'].isoformat(),
)
daily_cohort = []
for k, d in daily_rows:
    d['date'] = k
    daily_cohort.append(d)
daily_drill = render_drill(daily_drill_users)

# Weekly
weekly_rows, weekly_drill_users = build_period_cohorts(
    lambda d: f"{week_start(d).isoformat()}/{(week_start(d)+timedelta(days=6)).isoformat()}",
    lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}\u2013{datetime.fromisoformat(e).strftime('%b %-d')}")(*k.split('/')),
    lambda u: week_start(u['joined']).isoformat(),
)
weekly_cohort = []
for k, d in weekly_rows:
    d['week'] = k
    weekly_cohort.append(d)
weekly_drill = render_drill(weekly_drill_users)

# Monthly
monthly_rows, monthly_drill_users = build_period_cohorts(
    lambda d: month_key(d),
    lambda k: month_label(k),
    lambda u: month_key(u['joined']),
)
monthly_cohort = []
for k, d in monthly_rows:
    d['month'] = k
    monthly_cohort.append(d)
monthly_drill = render_drill(monthly_drill_users)


# Qualifying (campaigns with ≥30 funded across whole window)
camp_funded_total = Counter()
for u in users:
    if u['ftd']:
        camp_funded_total[u['campaign']] += 1
qualifying_campaigns = {c for c, n in camp_funded_total.items() if n >= 30}
print(f"  qualifying campaigns (≥30 funded): {len(qualifying_campaigns)} of {len(camp_funded_total)}")


def filter_drill_to_qual(drill):
    return {b: [r for r in rows if r['campaign'] in qualifying_campaigns]
            for b, rows in drill.items()}


def rollup_qual_period(rows, key_field):
    """Recompute aggregates from filtered drill rows."""
    out = []
    for r in rows:
        camps = []
        # We don't have raw users here — approximate by aggregating drill rows
    return out


def rebuild_qual_period(period_users, qual_set, period_key_fn, period_label_fn, key_field):
    """Filter users to qualifying campaigns, then re-aggregate per period."""
    bucket_signups   = defaultdict(int)
    bucket_funded_tt = defaultdict(list)
    bucket_deposit   = defaultdict(float)
    for u in period_users:
        if u['campaign'] not in qual_set: continue
        b = period_key_fn(u['joined'])
        bucket_signups[b] += 1
        if u['ftd'] and u['tt_fund'] is not None:
            bucket_funded_tt[b].append(u['tt_fund'])
            bucket_deposit[b] += u['deposit']
    rows = []
    for b in sorted(set(list(bucket_signups.keys()) + list(bucket_funded_tt.keys()))):
        d = bucket_dist(bucket_funded_tt[b])
        d['signups'] = bucket_signups.get(b, 0)
        d['total_deposit'] = round(bucket_deposit.get(b, 0), 2)
        d['label'] = period_label_fn(b)
        d[key_field] = b
        rows.append(d)
    return rows


qual_daily_cohort = rebuild_qual_period(
    users, qualifying_campaigns,
    lambda d: d.isoformat(),
    lambda k: datetime.strptime(k, '%Y-%m-%d').strftime('%b %-d'),
    'date')
qual_weekly_cohort = rebuild_qual_period(
    users, qualifying_campaigns,
    lambda d: f"{week_start(d).isoformat()}/{(week_start(d)+timedelta(days=6)).isoformat()}",
    lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}\u2013{datetime.fromisoformat(e).strftime('%b %-d')}")(*k.split('/')),
    'week')
qual_monthly_cohort = rebuild_qual_period(
    users, qualifying_campaigns,
    lambda d: month_key(d),
    lambda k: month_label(k),
    'month')

qual_daily_drill   = filter_drill_to_qual(daily_drill)
qual_weekly_drill  = filter_drill_to_qual(weekly_drill)
qual_monthly_drill = filter_drill_to_qual(monthly_drill)


# Top geos by funded
geo_funded = Counter()
for u in users:
    if u['ftd']: geo_funded[u['geo']] += 1
top_geos = [g for g, _ in geo_funded.most_common()]


COHORT_DATA = {
    'daily_cohort':            daily_cohort,
    'weekly_cohort':           weekly_cohort,
    'monthly_cohort':          monthly_cohort,
    'daily_campaign_drill':    daily_drill,
    'weekly_campaign_drill':   weekly_drill,
    'monthly_campaign_drill':  monthly_drill,
    'qual_daily_cohort':       qual_daily_cohort,
    'qual_weekly_cohort':      qual_weekly_cohort,
    'qual_monthly_cohort':     qual_monthly_cohort,
    'qual_daily_campaign_drill':   qual_daily_drill,
    'qual_weekly_campaign_drill':  qual_weekly_drill,
    'qual_monthly_campaign_drill': qual_monthly_drill,
    'qualifying_campaign_count':   len(qualifying_campaigns),
    'total_campaign_count':        len(camp_funded_total),
    'top_geos':                    top_geos,
}
print(f"  COHORT_DATA: daily={len(daily_cohort)} weekly={len(weekly_cohort)} monthly={len(monthly_cohort)}  qual={len(qualifying_campaigns)}/{len(camp_funded_total)}")


# ── 4. Build COHORT_ROAS ─────────────────────────────────────────────
print("\nBuilding COHORT_ROAS…")
def speed_buckets(month_users, snap=SNAPSHOT):
    """For users in this signup-month cohort, classify their FTD by months-from-signup."""
    m0 = m1 = m2 = m3p = 0
    d0 = d1 = d2 = d3p = 0.0
    for u in month_users:
        if not u['ftd']: continue
        # months between joined and ftd: 0 if same calendar month, etc.
        delta_months = (u['ftd'].year - u['joined'].year)*12 + (u['ftd'].month - u['joined'].month)
        if delta_months <= 0:    m0  += 1; d0  += u['deposit']
        elif delta_months == 1:  m1  += 1; d1  += u['deposit']
        elif delta_months == 2:  m2  += 1; d2  += u['deposit']
        else:                    m3p += 1; d3p += u['deposit']
    return {
        'm0':     {'count': m0,  'deposit': round(d0,  0)},
        'm1':     {'count': m1,  'deposit': round(d1,  0)},
        'm2':     {'count': m2,  'deposit': round(d2,  0)},
        'm3plus': {'count': m3p, 'deposit': round(d3p, 0)},
    }


# Map non-cohort campaign → spend used as the per-month spend proxy.
# We have no daily NC data to bucket spend by month, so we fall back to:
#   per-month cohort spend = NC spend × (cohort signups in month / total cohort signups)
total_cohort_signups = len(users)
cohort_signups_by_month = Counter(month_key(u['joined']) for u in users)


def roas_rows(user_subset):
    by_month_users = defaultdict(list)
    for u in user_subset:
        by_month_users[month_key(u['joined'])].append(u)
    out = []
    total_spend_subset = sum(nc_spend.get(c, 0) for c in {u['campaign'] for u in user_subset})
    total_signups_subset = sum(len(v) for v in by_month_users.values())
    for mk in sorted(by_month_users.keys()):
        mlist = by_month_users[mk]
        s = len(mlist)
        f = sum(1 for u in mlist if u['ftd'])
        dep = sum(u['deposit'] for u in mlist if u['ftd'])
        # spend allocated by signup share within subset
        sp = (s / total_signups_subset * total_spend_subset) if total_signups_subset else 0
        roas = round(dep / sp, 1) if sp else 0
        out.append({
            'month': mk,
            'label': month_label(mk),
            'spend': round(sp, 0),
            'signups': s,
            'funded': f,
            'fund_rate': round(100*f/s, 2) if s else 0,
            'total_deposit': round(dep, 0),
            'roas': roas,
            'speed': speed_buckets(mlist),
        })
    return out


cr_rows = roas_rows(users)
cr_geo  = {}
for geo in [g for g,_ in GEO_RULES]:
    sub = [u for u in users if u['geo'] == geo]
    if not sub: continue
    cr_geo[geo] = {'rows': roas_rows(sub)}
COHORT_ROAS = {'rows': cr_rows, 'geo_data': cr_geo}
print(f"  COHORT_ROAS rows: {len(cr_rows)}")


# ── 5. Build SCALING_DATA ────────────────────────────────────────────
print("\nBuilding SCALING_DATA…")
CPA_TARGET = 500   # funded CPA target ($)
EXP_LEARNING_BUDGET = 2000  # max spend for experiments before funded proof

def decision(funded, median_days, doas, type_, cpa=None, s2f=None,
             spend=0, deposit=0, signups=0, is_exp=False):
    """Weekly unlock rules:
    Scale  - funded CPA below target AND S2F (signup-to-funded) is stable
    Hold   - signups are coming but S2F is weak (leads but no conversion)
    Kill   - spend is high and funded=0, or deposit value is low
    Test   - experiment under learning budget without funded proof
    """
    s2f_str = (str(round(s2f, 2)) + '%') if s2f is not None else 'n/a'

    # Rule 4: Experiments under learning budget without funded proof
    if is_exp and funded < 5 and spend <= EXP_LEARNING_BUDGET:
        return 'Test', 'Experiment under learning budget; no funded conversion yet'
    if is_exp and funded == 0 and spend > EXP_LEARNING_BUDGET:
        return 'Kill', 'Experiment exceeded learning budget ($' + str(round(spend)) + ') with zero funded'

    # Rule 3: High spend + zero funded
    if funded == 0 and spend >= 500:
        return 'Kill', 'Spend $' + str(round(spend)) + ' with zero funded'
    # Rule 3b: Funded but deposit value very low
    if funded > 0 and deposit > 0 and spend > 500:
        dep_per_funded = deposit / funded
        if dep_per_funded < 20 and (cpa is None or cpa > CPA_TARGET * 2):
            return 'Kill', 'Low deposit ($' + str(round(dep_per_funded)) + '/user) + high CPA'

    # Rule 1: Scale - CPA below target AND S2F stable (>= 0.3%)
    if funded >= 5 and cpa is not None and cpa < CPA_TARGET:
        if s2f is not None and s2f >= 0.3:
            return 'Scale', 'CPA $' + str(round(cpa)) + ' < $' + str(CPA_TARGET) + '; S2F ' + s2f_str + ' stable'
        return 'Hold', 'CPA good ($' + str(round(cpa)) + ') but S2F weak (' + s2f_str + ')'

    # Rule 2: Hold - signups coming but S2F weak
    if signups >= 50 and (s2f is None or s2f < 0.3):
        if funded > 0:
            return 'Hold', str(signups) + ' signups but S2F ' + s2f_str + ' weak; watching conversion'
        return 'Hold', str(signups) + ' signups but no funded yet; watching'

    # Negative DOAS
    if doas is not None and doas < 0:
        return 'Kill', 'Negative DOAS (' + str(round(doas, 2)) + 'x)'

    # CPA way over target
    if funded > 0 and cpa is not None and cpa > CPA_TARGET * 2:
        return 'Kill', 'CPA $' + str(round(cpa)) + ' > 2x target'

    # Low funded volume
    if funded > 0 and funded < 5:
        return 'Hold', 'Low volume (' + str(funded) + ' funded); too early'

    if funded == 0:
        return 'Kill', 'No funded; cut spend'
    return 'Hold', 'Under review'


# Per-campaign cohort aggregates
camp_users = defaultdict(list)
for u in users:
    camp_users[u['campaign']].append(u)

# Union with NC-only campaigns ($ spend with no cohort users → still listed for completeness)
all_campaigns = set(camp_users.keys()) | set(nc_spend.keys())

# Noise floor: drop sub-signal rows so the table stays usable.
# Keep any campaign with cohort or NC funded; otherwise require ≥$250 spend.
NOISE_SPEND = 250.0

campaigns = []
for camp in sorted(all_campaigns):
    ulist  = camp_users.get(camp, [])
    funded_users = [u for u in ulist if u['ftd']]
    funded = len(funded_users)
    nc_f   = nc_funded.get(camp, 0)
    spend  = nc_spend.get(camp, 0)

    # Drop sub-noise: no spend, no cohort funded, no nc funded
    if funded == 0 and spend < NOISE_SPEND and nc_f == 0:
        continue

    if funded:
        tts = [u['tt_fund'] for u in funded_users if u['tt_fund'] is not None]
        median_days = round(statistics.median(tts), 1) if tts else None
        avg_days    = round(sum(tts)/len(tts), 1) if tts else None
    else:
        median_days = avg_days = None
    revenue = sum(u['pnl'] for u in funded_users) + nc_revenue.get(camp, 0)
    deposit = sum(u['deposit'] for u in funded_users)
    doas    = round(revenue / spend, 3) if spend else None
    geo     = classify_geo(camp)
    type_   = classify_type(camp)
    is_exp  = _is_exp_campaign(camp, type_)
    pic     = pic_for(camp, geo)
    signups = len(ulist)
    cpa     = round(spend / funded, 1) if funded > 0 and spend else None
    s2f     = round(100.0 * funded / signups, 2) if signups > 0 else None
    dec, rule = decision(funded, median_days, doas, type_, cpa=cpa, s2f=s2f,
                         spend=spend, deposit=deposit, signups=signups, is_exp=is_exp)
    campaigns.append({
        'campaign':   camp,
        'geo':        geo,
        'type':       type_,
        'funded':     funded,
        'signups':    signups,
        's2f':        s2f,
        'median_days':median_days,
        'avg_days':   avg_days,
        'revenue':    round(revenue, 2),
        'spend':      round(spend, 2),
        'total_deposit': round(deposit, 2),
        'doas':       doas,
        'decision':   dec,
        'rule':       rule,
        'pic':        pic,
    })


# Scoreboard — SIGNUP-COHORT definition so SD reconciles with Cohort Speed.
#
# Cohort Speed monthly_cohort row "Apr 2026" funded = Apr-signup users who
# went on to fund (any date). The Scaling Decisions cohort metric must
# equal that, not "users whose first_deposit_date was in April".
#
# Per-PIC scoreboard EXCLUDES ROW/ROW1/ROW2 shared brand-keyword campaigns:
# those aggregate signups across many countries and bias whichever PIC
# nominally owns them. Headline cohort numbers still include ROW (for
# Cohort Speed reconciliation); only the per-PIC accountability rows
# strip them out.
SHARED_GEOS = {'ROW', 'ROW1', 'ROW2'}

def is_baseline_signup(d):
    """Signup falls in the baseline period. Uses all months before the target
    month in the same year. If target is January, uses the full prior year."""
    if TARGET_MONTH == 1:
        return d.year == TARGET_YEAR - 1
    return d.year == TARGET_YEAR and 1 <= d.month < TARGET_MONTH

def is_target_month_signup(d):
    """Signup falls in the target (current) month."""
    return d.year == TARGET_YEAR and d.month == TARGET_MONTH

# Aliases for backward compatibility within this script
is_q1_signup  = is_baseline_signup
is_apr_signup = is_target_month_signup

def is_same_month_funded(u):
    """BI-aligned definition: user funded in the same calendar month they signed up."""
    if not u['ftd'] or not u['joined']:
        return False
    return u['ftd'].year == u['joined'].year and u['ftd'].month == u['joined'].month

# Total cohort funded per period — SAME-MONTH definition (aligns with BI team).
# Only counts users who funded within the same calendar month they signed up.
q1_cf_total  = sum(1 for u in users if is_same_month_funded(u) and is_q1_signup(u['joined']))
apr_cf_total = sum(1 for u in users if is_same_month_funded(u) and is_apr_signup(u['joined']))

# Per-PIC counters, EXCLUDING shared ROW campaigns.
# Uses same-month funded definition (BI-aligned).
q1_funded_by_pic   = Counter()
apr_funded_by_pic  = Counter()
# Each PIC also has a "shared" geo (ROW for Aswathi, ROW1 for Ishank, ROW2
# for Conor). Track that contribution separately so we can show on each row
# how much of their funded comes from shared brand campaigns vs their tiers.
shared_q1_by_pic   = Counter()
shared_apr_by_pic  = Counter()
shared_q1   = 0
shared_apr  = 0
for u in users:
    if not is_same_month_funded(u): continue
    if u['geo'] in SHARED_GEOS and u['campaign'] not in CAMPAIGN_PIC_OVERRIDES:
        if is_q1_signup(u['joined']):  shared_q1  += 1
        if is_apr_signup(u['joined']): shared_apr += 1
        owner_pic = GEO_TO_PIC.get(u['geo'])
        if owner_pic:
            if is_q1_signup(u['joined']):  shared_q1_by_pic[owner_pic]  += 1
            if is_apr_signup(u['joined']): shared_apr_by_pic[owner_pic] += 1
        continue
    pic = pic_for(u['campaign'], u['geo'])
    if is_q1_signup(u['joined']):   q1_funded_by_pic[pic]  += 1
    if is_apr_signup(u['joined']):  apr_funded_by_pic[pic] += 1

# HEADLINE totals (must reconcile with Cohort Speed, COHORT_ROAS,
# DATA.cohort_monthly_funded) — INCLUDES ROW shared campaigns.
q1_cf             = q1_cf_total
apr_cohort_actual = apr_cf_total

# PIC-attributable totals (excludes ROW) — used for per-PIC accountability.
q1_cf_pic     = sum(q1_funded_by_pic.values())
apr_cohort_pic = sum(apr_funded_by_pic.values())

# Read truth-source totals from DATA.m_funnel for reconciliation:
#   - Q1 NC funded = sum of Jan/Feb/Mar funded across all sources
#   - Apr NC actual = Apr m_funnel.total.funded (this is the "Cumul. Funded"
#     the Overall Funnel shows; same number Scaling Decisions tracks against
#     the 3,000 month-end target).
def _read_data_mfunnel():
    html = pathlib.Path(HTML_PATH).read_text()
    i  = html.find('const DATA = ')
    if i < 0: i = html.find('var DATA = ')
    if i < 0: return None
    i2 = html.find('{', i)
    depth = 0; end = -1
    for k in range(i2, len(html)):
        c = html[k]
        if c == '{': depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0: end = k; break
    try:
        return json.loads(html[i2:end+1])
    except Exception:
        return None

_data_blob = _read_data_mfunnel()
m_funnel = (_data_blob or {}).get('m_funnel', [])

def _mfunnel_funded(label):
    for row in m_funnel:
        if row.get('label') == label:
            return int((row.get('total') or {}).get('funded') or 0)
    return None

# Source of truth for NC counts — dynamically computed from month keys.
# Baseline months = all months before target month (prior year if target is Jan).
# Target month = the month we're tracking toward NC_MONTHLY_TARGET.
if TARGET_MONTH == 1:
    baseline_month_keys = [f"{TARGET_YEAR-1:04d}-{m:02d}" for m in range(1, 13)]
else:
    baseline_month_keys = [f"{TARGET_YEAR:04d}-{m:02d}" for m in range(1, TARGET_MONTH)]
target_month_key = f"{TARGET_YEAR:04d}-{TARGET_MONTH:02d}"

if nc_has_dates:
    q1_nc_from_csv  = sum(nc_funded_by_month.get(m, 0) for m in baseline_month_keys)
    target_nc_from_csv = nc_funded_by_month.get(target_month_key, 0)
    q1_nc            = q1_nc_from_csv
    apr_nc_from_data = target_nc_from_csv
    print(f"  NC funded — baseline ({baseline_month_keys[0]}–{baseline_month_keys[-1]}): {q1_nc_from_csv:,}  target month ({target_month_key}): {target_nc_from_csv:,}  (direct from NC CSV)")
else:
    baseline_labels = [datetime(TARGET_YEAR, m, 1).strftime('%b %Y') for m in range(1, TARGET_MONTH)]
    target_label    = datetime(TARGET_YEAR, TARGET_MONTH, 1).strftime('%b %Y')
    q1_nc_from_data = sum(filter(None, [_mfunnel_funded(lbl) for lbl in baseline_labels]))
    apr_nc_from_data = _mfunnel_funded(target_label) or 0
    q1_nc = q1_nc_from_data if q1_nc_from_data else 0

# Cohort target derivation — baseline ratio calibration.
# Uses the BASELINE period's avg same-month-cohort / NC ratio (not the current
# month's partial data, which is artificially low early in the month because
# cohort users haven't had time to convert yet).
target_nc_actual = apr_nc_from_data if nc_has_dates else (apr_nc_from_data or 0)

# Compute baseline ratio from complete months (Jan-Apr etc.)
baseline_cohort_months = {}
for u in users:
    jm = u['joined'].strftime('%Y-%m')
    if jm in set(baseline_month_keys) and is_same_month_funded(u):
        baseline_cohort_months[jm] = baseline_cohort_months.get(jm, 0) + 1

baseline_ratios = []
for mk in baseline_month_keys:
    coh_m = baseline_cohort_months.get(mk, 0)
    nc_m = nc_funded_by_month.get(mk, 0) if nc_has_dates else 0
    if nc_m > 0 and coh_m > 0:
        baseline_ratios.append(coh_m / nc_m)
        print(f"  Baseline ratio {mk}: {coh_m}/{nc_m} = {coh_m/nc_m:.3f}")

if baseline_ratios:
    target_ratio = sum(baseline_ratios) / len(baseline_ratios)
    print(f"  Baseline avg ratio ({len(baseline_ratios)} months): {target_ratio:.3f}")
else:
    target_ratio = (apr_cohort_actual / target_nc_actual) if target_nc_actual else 0
    print(f"  No baseline data; falling back to target-month ratio: {target_ratio:.3f}")

cohort_target = round(NC_MONTHLY_TARGET * target_ratio)

# PIC-attributable target — same ratio scaled by PIC share of cohort
pic_share = (apr_cohort_pic / apr_cohort_actual) if apr_cohort_actual else 1.0
target_ratio_pic = target_ratio * pic_share
cohort_target_pic = round(NC_MONTHLY_TARGET * target_ratio_pic)
print(f"  Cohort target: {NC_MONTHLY_TARGET:,} x {target_ratio:.3f} = {cohort_target}")
print(f"  PIC-attributable target: {cohort_target_pic} (PIC share of cohort = {pic_share:.1%})")

# Per-(PIC, region) q1 and apr funded counters — same-month definition.
# We key by (pic, geo) instead of just geo so override campaigns (e.g.
# Conor's africa-bing Other-geo campaigns) get attributed to their assigned
# PIC — Conor's region row will include "Other" alongside his Africa tiers,
# summing correctly to his PIC target.
q1_pic_region  = defaultdict(int)
apr_pic_region = defaultdict(int)
for u in users:
    if not is_same_month_funded(u): continue
    if u['geo'] in SHARED_GEOS and u['campaign'] not in CAMPAIGN_PIC_OVERRIDES:
        continue
    p = pic_for(u['campaign'], u['geo'])
    if is_q1_signup(u['joined']):  q1_pic_region[(p, u['geo'])]  += 1
    if is_apr_signup(u['joined']): apr_pic_region[(p, u['geo'])] += 1


scoreboard = []
for pic in ['Conor Horan', 'Aswathi Unni', 'Ishank Naik']:
    q1f   = q1_funded_by_pic.get(pic, 0)
    # Q1 share is over PIC-attributable Q1 (sums to 100% across PICs)
    share = (q1f / q1_cf_pic * 100) if q1_cf_pic else 0
    # Target is share × PIC-attributable cohort target (sums to cohort_target_pic)
    apr_target = round(share/100 * cohort_target_pic)
    apr_actual = apr_funded_by_pic.get(pic, 0)
    pct = round(100*apr_actual/apr_target, 1) if apr_target else 0
    # PIC's shared-campaign (ROW/ROW1/ROW2) contribution
    shared_geos_owned = [g for g in PIC_MAP[pic] if g in SHARED_GEOS]
    shared_q1_pic  = shared_q1_by_pic.get(pic, 0)
    shared_apr_pic = shared_apr_by_pic.get(pic, 0)
    # Per-region breakdown — each region's target is region's share of PIC's
    # Q1 funded × the PIC's apr_target. Sums to the PIC's apr_target.
    # Use only geos that actually contribute Q1 funded to this PIC (handles
    # override campaigns whose geo isn't in PIC_MAP).
    pic_regions = sorted(set(g for (p, g) in q1_pic_region if p == pic))
    region_rows = []
    for region in pic_regions:
        r_q1  = q1_pic_region.get((pic, region), 0)
        r_apr = apr_pic_region.get((pic, region), 0)
        r_share = (r_q1 / q1f * 100) if q1f else 0
        r_target = round(r_share/100 * apr_target)
        r_pct = round(100*r_apr/r_target, 1) if r_target else 0
        region_rows.append({
            'region':     region,
            'q1_funded':  r_q1,
            'q1_share':   round(r_share, 1),
            'apr_target': r_target,
            'apr_actual': r_apr,
            'pct':        r_pct,
        })
    region_rows.sort(key=lambda r: -r['q1_funded'])
    scoreboard.append({
        'pic':              pic,
        'geos':             pic_regions,
        'shared_geos':      shared_geos_owned,
        'q1_funded':        q1f,
        'q1_share':         round(share, 1),
        'apr_target':       apr_target,
        'apr_actual':       apr_actual,
        'pct':              pct,
        'shared_q1_funded': shared_q1_pic,
        'shared_apr_funded':shared_apr_pic,
        'regions':          region_rows,
    })

## Period buckets (campaigns × period) for the period-filter view ───────────
def bucket_users_to_periods(period_key_fn, period_label_fn, period_kind):
    """Group users by period bucket → per campaign aggregate within bucket."""
    out_buckets = defaultdict(lambda: defaultdict(list))   # period_key -> camp -> users
    for u in users:
        pk = period_key_fn(u['joined'])
        out_buckets[pk][u['campaign']].append(u)
    res = []
    for pk in sorted(out_buckets.keys()):
        bcamps = []
        camp_dict = out_buckets[pk]
        # Allocate spend by signup share within this period bucket
        signups_in_bucket = sum(len(v) for v in camp_dict.values())
        for camp in sorted(camp_dict.keys()):
            ulist = camp_dict[camp]
            funded_users = [u for u in ulist if u['ftd']]
            funded = len(funded_users)
            tts = [u['tt_fund'] for u in funded_users if u['tt_fund'] is not None]
            md  = round(statistics.median(tts), 1) if tts else None
            ad  = round(sum(tts)/len(tts), 1) if tts else None
            sd_  = sum(1 for x in tts if x == 0)
            w7   = sum(1 for x in tts if x is not None and x <= 7)
            w14  = sum(1 for x in tts if x is not None and x <= 14)
            w30  = sum(1 for x in tts if x is not None and x <= 30)
            o30  = sum(1 for x in tts if x is not None and x > 30)
            rev  = sum(u['pnl'] for u in funded_users)
            dep  = sum(u['deposit'] for u in funded_users)
            # Spend per period: pro-rate full-window NC spend by share of signups in this bucket
            full_camp_signups = sum(1 for u in users if u['campaign'] == camp)
            sp = nc_spend.get(camp, 0) * (len(ulist) / full_camp_signups) if full_camp_signups else 0
            doas = round(rev/sp, 3) if sp else None
            geo_ = classify_geo(camp)
            type_ = classify_type(camp)
            is_exp_ = _is_exp_campaign(camp, type_)
            signups_ = len(ulist)
            cpa_ = round(sp / funded, 1) if funded > 0 and sp else None
            s2f_ = round(100.0 * funded / signups_, 2) if signups_ > 0 else None
            dec_, rule_ = decision(funded, md, doas, type_, cpa=cpa_, s2f=s2f_,
                                   spend=sp, deposit=dep, signups=signups_, is_exp=is_exp_)
            if funded == 0 and sp < 25 and rev == 0:
                continue
            bcamps.append({
                'campaign':    camp,
                'geo':         geo_,
                'type':        type_,
                'funded':      funded,
                'signups':     signups_,
                's2f':         s2f_,
                'median_days': md,
                'avg_days':    ad,
                'same_day':    sd_,
                'within_7d':   w7,
                'within_14d':  w14,
                'within_30d':  w30,
                'over_30d':    o30,
                'revenue':     round(rev, 2),
                'spend':       round(sp, 2),
                'total_deposit': round(dep, 2),
                'doas':        doas,
                'decision':    dec_,
                'rule':        rule_,
                'pic':         pic_for(camp, geo_),
                'nc_funded':   0,
            })
        res.append({'key': pk, 'label': period_label_fn(pk), 'campaigns': bcamps})
    return res


def quarter_key(d):
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


# Pre-compute full-camp signups for perf
full_signups_by_camp = Counter(u['campaign'] for u in users)
# Override the inner computation with cached full count
def _bucket_periods_fast(period_key_fn, period_label_fn):
    out = defaultdict(lambda: defaultdict(list))
    for u in users:
        out[period_key_fn(u['joined'])][u['campaign']].append(u)
    res = []
    for pk in sorted(out.keys()):
        bcamps = []
        for camp in sorted(out[pk].keys()):
            ulist = out[pk][camp]
            funded_users = [u for u in ulist if u['ftd']]
            funded = len(funded_users)
            tts = [u['tt_fund'] for u in funded_users if u['tt_fund'] is not None]
            md  = round(statistics.median(tts), 1) if tts else None
            ad  = round(sum(tts)/len(tts), 1) if tts else None
            sd_  = sum(1 for x in tts if x == 0)
            w7   = sum(1 for x in tts if x is not None and x <= 7)
            w14  = sum(1 for x in tts if x is not None and x <= 14)
            w30  = sum(1 for x in tts if x is not None and x <= 30)
            o30  = sum(1 for x in tts if x is not None and x > 30)
            rev  = sum(u['pnl'] for u in funded_users)
            dep  = sum(u['deposit'] for u in funded_users)
            full = full_signups_by_camp.get(camp, 0)
            sp = nc_spend.get(camp, 0) * (len(ulist) / full) if full else 0
            doas = round(rev/sp, 3) if sp else None
            geo_ = classify_geo(camp)
            type_ = classify_type(camp)
            is_exp_ = _is_exp_campaign(camp, type_)
            signups_ = len(ulist)
            cpa_ = round(sp / funded, 1) if funded > 0 and sp else None
            s2f_ = round(100.0 * funded / signups_, 2) if signups_ > 0 else None
            dec_, rule_ = decision(funded, md, doas, type_, cpa=cpa_, s2f=s2f_,
                                   spend=sp, deposit=dep, signups=signups_, is_exp=is_exp_)
            if funded == 0 and sp < 25 and rev == 0:
                continue
            bcamps.append({
                'campaign': camp, 'geo': geo_, 'type': type_,
                'funded': funded, 'signups': signups_, 's2f': s2f_,
                'median_days': md, 'avg_days': ad,
                'same_day': sd_, 'within_7d': w7, 'within_14d': w14,
                'within_30d': w30, 'over_30d': o30,
                'revenue': round(rev, 2), 'spend': round(sp, 2),
                'total_deposit': round(dep, 2),
                'doas': doas, 'decision': dec_, 'rule': rule_,
                'pic': pic_for(camp, geo_), 'nc_funded': 0,
            })
        res.append({'key': pk, 'label': period_label_fn(pk), 'campaigns': bcamps})
    return res


periods_daily   = _bucket_periods_fast(lambda d: d.isoformat(),
    lambda k: datetime.strptime(k,'%Y-%m-%d').strftime('%b %-d'))
periods_weekly  = _bucket_periods_fast(
    lambda d: f"{week_start(d).isoformat()}/{(week_start(d)+timedelta(days=6)).isoformat()}",
    lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}\u2013{datetime.fromisoformat(e).strftime('%b %-d')}")(*k.split('/')))
periods_monthly = _bucket_periods_fast(lambda d: month_key(d), lambda k: month_label(k))
periods_quarterly = _bucket_periods_fast(lambda d: quarter_key(d),
    lambda k: f"{k.split('-')[1]} {k.split('-')[0]}")
periods_all     = [{'key': 'all', 'label': f"Full Window ({WINDOW_START.strftime('%b %-d')} \u2013 {WINDOW_END.strftime('%b %-d %Y')})", 'campaigns': campaigns}]

print(f"  Period buckets — daily:{len(periods_daily)} weekly:{len(periods_weekly)} monthly:{len(periods_monthly)} quarterly:{len(periods_quarterly)}")


## Per-PIC reallocation hint = sum of Kill-decision spend in PIC's geos ──
pic_kill_spend = defaultdict(float)
for c in campaigns:
    if c['decision'] == 'Kill':
        pic_kill_spend[c['pic']] += c['spend']

## NC actual for the target month. Falls back to a ratio estimate if NC CSV
## doesn't cover the target month.
if target_nc_actual:
    apr_nc_actual_est = target_nc_actual
else:
    apr_nc_actual_est = round(apr_cohort_actual * (q1_nc / q1_cf)) if q1_cf else 0


## Pace calculations ────────────────────────────────────────────────────
day_of_month   = SNAPSHOT.day if SNAPSHOT.month == TARGET_MONTH else DAYS_IN_TARGET_MONTH
days_remaining = max(1, DAYS_IN_TARGET_MONTH - day_of_month)


## Enrich scoreboard with pace + reallocation
for s in scoreboard:
    s['kill_spend']     = round(pic_kill_spend.get(s['pic'], 0), 0)
    s['actual_per_day'] = round(s['apr_actual'] / day_of_month, 1) if day_of_month else 0
    gap                  = max(0, s['apr_target'] - s['apr_actual'])
    s['needed_per_day'] = round(gap / days_remaining, 1) if days_remaining else 0
    s['gap']            = s['apr_actual'] - s['apr_target']

## Add Unassigned scoreboard row only if there are actually unassigned
## campaigns (i.e., geos that don't map to any PIC). With ROW as the
## catch-all this should be zero and the row is suppressed.
unassigned_q1   = sum(1 for u in users if is_same_month_funded(u) and is_q1_signup(u['joined'])  and GEO_TO_PIC.get(u['geo'], 'Unassigned') == 'Unassigned')
unassigned_apr  = sum(1 for u in users if is_same_month_funded(u) and is_apr_signup(u['joined']) and GEO_TO_PIC.get(u['geo'], 'Unassigned') == 'Unassigned')
unassigned_camps = sum(1 for c in campaigns if c['pic'] == 'Unassigned')
if unassigned_camps > 0:
    total_q1_with_unassigned = q1_cf + unassigned_q1
    unassigned_share = (unassigned_q1 / total_q1_with_unassigned * 100) if total_q1_with_unassigned else 0
    unassigned_target = round(unassigned_share/100 * cohort_target)
    scoreboard.append({
        'pic':            'Unassigned',
        'geos':           ['row-/Other campaigns'],
        'q1_funded':      unassigned_q1,
        'q1_share':       round(unassigned_share, 1),
        'apr_target':     unassigned_target,
        'apr_actual':     unassigned_apr,
        'pct':            round(100*unassigned_apr/unassigned_target, 1) if unassigned_target else 0,
        'kill_spend':     round(pic_kill_spend.get('Unassigned', 0), 0),
        'actual_per_day': round(unassigned_apr / day_of_month, 1) if day_of_month else 0,
        'needed_per_day': round(max(0, unassigned_target - unassigned_apr) / days_remaining, 1) if days_remaining else 0,
        'gap':            unassigned_apr - unassigned_target,
    })


# Headline keys (cohort_target, apr_cohort_actual, q1_cf) include ROW — these
# are what the dashboard's reconciliation invariants check against. The _pic
# variants exclude ROW and drive the per-PIC accountability rows. shared_*
# capture the ROW carve-out that's been excluded from per-PIC.
SCALING_DATA = {
    'campaigns':          campaigns,
    'scoreboard':         scoreboard,
    'apr_nc_target':      NC_MONTHLY_TARGET,
    'apr_nc_actual':      apr_nc_actual_est,
    'cohort_target':      cohort_target,
    'cohort_target_pic':  cohort_target_pic,
    'apr_cohort_actual':  apr_cohort_actual,
    'apr_cohort_pic':     apr_cohort_pic,
    'q1_cf':              q1_cf,
    'q1_cf_pic':          q1_cf_pic,
    'q1_nc':              q1_nc,
    'shared_q1':          shared_q1,
    'shared_apr':         shared_apr,
    'shared_geos':        sorted(SHARED_GEOS),
    'snapshot_date':      SNAPSHOT.isoformat(),
    'day_of_month':       day_of_month,
    'days_in_month':      DAYS_IN_TARGET_MONTH,
    'days_in_april':      DAYS_IN_TARGET_MONTH,
    'days_remaining':     days_remaining,
    'target_month':       TARGET_MONTH,
    'target_year':        TARGET_YEAR,
    'baseline_months':    TARGET_MONTH - 1,
    'window':             {'start': WINDOW_START.isoformat(), 'end': WINDOW_END.isoformat()},
    'periods': {
        'daily':     periods_daily,
        'weekly':    periods_weekly,
        'monthly':   periods_monthly,
        'quarterly': periods_quarterly,
        'all':       periods_all,
    },
}
print(f"  SCALING_DATA: campaigns={len(campaigns)}  apr_actual={apr_cohort_actual}  q1_cf={q1_cf}  cohort_target={cohort_target}  apr_nc_actual≈{apr_nc_actual_est}")
for s in scoreboard:
    print(f"    {s['pic']}: q1={s['q1_funded']} ({s['q1_share']}%) target={s['apr_target']} actual={s['apr_actual']} ({s['pct']}%) kill_spend=${s['kill_spend']:,.0f}")


# ── 5b. Build Overall Funnel structures from NC CSV ─────────────────
# When the NC CSV has dates we own the Overall Funnel data directly:
# m_funnel (monthly), w_funnel (weekly), daily_funnel (daily) plus the
# top-level totals. Each entry has core / exp / total breakdowns.
overall_data_blocks = {}
if nc_has_dates and nc_rows:
    print("\nBuilding Overall Funnel data from NC CSV…")
    def _agg(rows):
        d = sum(r['demo'] for r in rows)
        rl = sum(r['real'] for r in rows)
        f = sum(r['funded'] for r in rows)
        sp = round(sum(r['spend'] for r in rows), 2)
        rv = round(sum(r['revenue'] for r in rows), 2)
        return {
            'demo':    d,
            'real':    rl,
            'funded':  f,
            'spend':   sp,
            'revenue': rv,
            'roas':    round(rv/sp, 2) if sp > 0 else None,
            'cpa':     round(sp/f, 1)  if f  > 0 else None,
            'd2r':     round(100*rl/d, 1) if d > 0 else None,
            'r2f':     round(100*f/rl, 1) if rl > 0 else None,
        }

    def _bucket_funnel(period_key_fn, period_label_fn, sort_buckets=True):
        buckets = defaultdict(list)
        for r in nc_rows:
            buckets[period_key_fn(r['date'])].append(r)
        keys = sorted(buckets.keys()) if sort_buckets else list(buckets.keys())
        out = []
        for k in keys:
            rows = buckets[k]
            core_rows = [r for r in rows if not r['is_exp']]
            exp_rows  = [r for r in rows if     r['is_exp']]
            out.append({
                'label': period_label_fn(k),
                'core':  _agg(core_rows),
                'exp':   _agg(exp_rows),
                'total': _agg(rows),
            })
        return out

    def _bucket_brand(period_key_fn, period_label_fn, brand_only):
        buckets = defaultdict(list)
        for r in nc_rows:
            if r['is_brand'] != brand_only:
                continue
            buckets[period_key_fn(r['date'])].append(r)
        keys = sorted(buckets.keys())
        out = []
        for k in keys:
            rows = buckets[k]
            core_rows = [r for r in rows if not r['is_exp']]
            exp_rows  = [r for r in rows if     r['is_exp']]
            out.append({
                'label': period_label_fn(k),
                'core':  _agg(core_rows),
                'exp':   _agg(exp_rows),
                'total': _agg(rows),
            })
        return out

    m_funnel = _bucket_funnel(lambda d: month_key(d), lambda k: month_label(k))
    w_funnel = _bucket_funnel(
        lambda d: week_start(d).isoformat(),
        lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}–{datetime.fromisoformat(e).strftime('%b %-d')}")(k, (datetime.fromisoformat(k)+timedelta(days=6)).isoformat()))

    # Daily funnel — one entry per date
    daily_funnel = []
    by_date = defaultdict(list)
    for r in nc_rows:
        by_date[r['date']].append(r)
    for d in sorted(by_date.keys()):
        rows = by_date[d]
        core_rows = [r for r in rows if not r['is_exp']]
        exp_rows  = [r for r in rows if     r['is_exp']]
        daily_funnel.append({
            'date':  d.isoformat(),
            'label': d.strftime('%b %-d'),
            'core':  _agg(core_rows),
            'exp':   _agg(exp_rows),
            'total': _agg(rows),
        })

    # Brand / non-brand splits (UI tabs are gone but renderers may still
    # reference these; provide them so render code doesn't crash)
    m_funnel_brand    = _bucket_brand(lambda d: month_key(d), lambda k: month_label(k), True)
    m_funnel_nonbrand = _bucket_brand(lambda d: month_key(d), lambda k: month_label(k), False)
    w_funnel_brand    = _bucket_brand(
        lambda d: week_start(d).isoformat(),
        lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}–{datetime.fromisoformat(e).strftime('%b %-d')}")(k, (datetime.fromisoformat(k)+timedelta(days=6)).isoformat()),
        True)
    w_funnel_nonbrand = _bucket_brand(
        lambda d: week_start(d).isoformat(),
        lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}–{datetime.fromisoformat(e).strftime('%b %-d')}")(k, (datetime.fromisoformat(k)+timedelta(days=6)).isoformat()),
        False)

    # Top-level KPI totals
    total_funded = sum(r['funded'] for r in nc_rows)
    core_funded  = sum(r['funded'] for r in nc_rows if not r['is_exp'])
    exp_funded   = sum(r['funded'] for r in nc_rows if     r['is_exp'])
    brand_funded    = sum(r['funded'] for r in nc_rows if     r['is_brand'])
    nonbrand_funded = sum(r['funded'] for r in nc_rows if not r['is_brand'])

    # Per-geo daily structures the renderer reads from. Each entry has the
    # core/exp split shape: {date, label, c_demo, e_demo, c_real, e_real,
    # c_funded, e_funded, c_spend, e_spend, c_rev, e_rev}.
    def _build_daily_geo(filter_fn=None):
        out = defaultdict(lambda: defaultdict(lambda: {
            'c_demo':0,'e_demo':0,'c_real':0,'e_real':0,
            'c_funded':0,'e_funded':0,'c_spend':0.0,'e_spend':0.0,
            'c_rev':0.0,'e_rev':0.0,
        }))
        for r in nc_rows:
            if filter_fn is not None and not filter_fn(r): continue
            slot = out[r['geo']][r['date']]
            pre = 'e_' if r['is_exp'] else 'c_'
            slot[pre+'demo']   += r['demo']
            slot[pre+'real']   += r['real']
            slot[pre+'funded'] += r['funded']
            slot[pre+'spend']  += r['spend']
            slot[pre+'rev']    += r['revenue']
        # Convert to {geo: [{date, label, ...}]} sorted by date
        result = {}
        for geo, by_date in out.items():
            arr = []
            for d in sorted(by_date.keys()):
                e = by_date[d]
                arr.append({
                    'date':     d.isoformat(),
                    'label':    d.strftime('%b %-d'),
                    'c_demo':   e['c_demo'],   'e_demo':   e['e_demo'],
                    'c_real':   e['c_real'],   'e_real':   e['e_real'],
                    'c_funded': e['c_funded'], 'e_funded': e['e_funded'],
                    'c_spend':  round(e['c_spend'], 2), 'e_spend': round(e['e_spend'], 2),
                    'c_rev':    round(e['c_rev'], 2),   'e_rev':   round(e['e_rev'], 2),
                })
            result[geo] = arr
        return result

    daily_geo          = _build_daily_geo()
    daily_geo_brand    = _build_daily_geo(lambda r: r['is_brand'])
    daily_geo_nonbrand = _build_daily_geo(lambda r: not r['is_brand'])

    # Per-geo monthly/weekly aggregates: {geo: {total_all, total_core, total_exp, periods:[{label, core, exp, total}]}}
    def _build_geo_periods(period_key_fn, period_label_fn, filter_fn=None):
        # Group rows: {geo: {bucket: [rows]}}
        out = defaultdict(lambda: defaultdict(list))
        all_by_geo = defaultdict(list)
        for r in nc_rows:
            if filter_fn is not None and not filter_fn(r): continue
            out[r['geo']][period_key_fn(r['date'])].append(r)
            all_by_geo[r['geo']].append(r)
        result = {}
        for geo, buckets in out.items():
            periods = []
            for k in sorted(buckets.keys()):
                rows = buckets[k]
                core_rows = [x for x in rows if not x['is_exp']]
                exp_rows  = [x for x in rows if     x['is_exp']]
                periods.append({
                    'label': period_label_fn(k),
                    'core':  _agg(core_rows),
                    'exp':   _agg(exp_rows),
                    'total': _agg(rows),
                })
            all_rows = all_by_geo[geo]
            core_all = [x for x in all_rows if not x['is_exp']]
            exp_all  = [x for x in all_rows if     x['is_exp']]
            result[geo] = {
                'total_all':  _agg(all_rows),
                'total_core': _agg(core_all),
                'total_exp':  _agg(exp_all),
                'periods':    periods,
            }
        return result

    m_geo          = _build_geo_periods(lambda d: month_key(d), lambda k: month_label(k))
    w_geo          = _build_geo_periods(
        lambda d: week_start(d).isoformat(),
        lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}–{datetime.fromisoformat(e).strftime('%b %-d')}")(k, (datetime.fromisoformat(k)+timedelta(days=6)).isoformat()))
    m_geo_brand    = _build_geo_periods(lambda d: month_key(d), lambda k: month_label(k), lambda r: r['is_brand'])
    m_geo_nonbrand = _build_geo_periods(lambda d: month_key(d), lambda k: month_label(k), lambda r: not r['is_brand'])
    w_geo_brand    = _build_geo_periods(
        lambda d: week_start(d).isoformat(),
        lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}–{datetime.fromisoformat(e).strftime('%b %-d')}")(k, (datetime.fromisoformat(k)+timedelta(days=6)).isoformat()),
        lambda r: r['is_brand'])
    w_geo_nonbrand = _build_geo_periods(
        lambda d: week_start(d).isoformat(),
        lambda k: (lambda s,e: f"{datetime.fromisoformat(s).strftime('%b %-d')}–{datetime.fromisoformat(e).strftime('%b %-d')}")(k, (datetime.fromisoformat(k)+timedelta(days=6)).isoformat()),
        lambda r: not r['is_brand'])

    # Campaign counts (top of Overall Funnel KPI strip)
    core_campaigns = len({r['campaign'] for r in nc_rows if not r['is_exp']})
    exp_campaigns  = len({r['campaign'] for r in nc_rows if     r['is_exp']})

    # geo_ranks — sorted by total funded desc
    geo_funded_totals = {g: sum(x['funded'] for x in nc_rows if x['geo']==g) for g in {x['geo'] for x in nc_rows}}
    geo_ranks = sorted(geo_funded_totals.keys(), key=lambda g: -geo_funded_totals[g])

    overall_data_blocks = {
        'm_funnel':           m_funnel,
        'w_funnel':           w_funnel,
        'daily_funnel':       daily_funnel,
        'daily_funnel_full':  daily_funnel,
        'm_funnel_brand':     m_funnel_brand,
        'w_funnel_brand':     w_funnel_brand,
        'm_funnel_nonbrand':  m_funnel_nonbrand,
        'w_funnel_nonbrand':  w_funnel_nonbrand,
        'total_funded':       total_funded,
        'core_funded':        core_funded,
        'exp_funded':         exp_funded,
        'brand_funded':       brand_funded,
        'nonbrand_funded':    nonbrand_funded,
        'daily_geo':          daily_geo,
        'daily_geo_brand':    daily_geo_brand,
        'daily_geo_nonbrand': daily_geo_nonbrand,
        'm_geo':              m_geo,
        'w_geo':              w_geo,
        'm_geo_brand':        m_geo_brand,
        'w_geo_brand':        w_geo_brand,
        'm_geo_nonbrand':     m_geo_nonbrand,
        'w_geo_nonbrand':     w_geo_nonbrand,
        'core_campaigns':     core_campaigns,
        'exp_campaigns':      exp_campaigns,
        'geo_ranks':          geo_ranks,
    }
    print(f"  m_funnel:     {len(m_funnel)} months  (Apr total funded = {next((m['total']['funded'] for m in m_funnel if m['label']=='Apr 2026'), '?')})")
    print(f"  w_funnel:     {len(w_funnel)} weeks")
    print(f"  daily_funnel: {len(daily_funnel)} days")
    print(f"  totals: total={total_funded:,}  core={core_funded:,}  exp={exp_funded:,}  brand={brand_funded:,}  nonbrand={nonbrand_funded:,}")


# ── 6. Inject into HTML ──────────────────────────────────────────────
print("\nInjecting into HTML…")
html = pathlib.Path(HTML_PATH).read_text()

def replace_var(html, varname, payload, sentinels=None):
    payload_js = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)
    if sentinels:
        start, end = sentinels
        pat = re.compile(re.escape(start) + r'.*?' + re.escape(end), re.S)
        repl = f"{start}\nvar {varname} = {payload_js};\n{end}"
        new, n = pat.subn(repl, html, count=1)
        if n != 1:
            raise RuntimeError(f"Sentinel replacement failed for {varname}")
        return new
    # walk braces
    i = html.find(f'var {varname} = ')
    if i < 0: raise RuntimeError(f"var {varname} not found")
    i2 = html.find('{', i)
    depth = 0; end_idx = -1
    for k in range(i2, len(html)):
        c = html[k]
        if c == '{': depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                end_idx = k; break
    # find trailing ;
    sc = html.find(';', end_idx)
    return html[:i] + f"var {varname} = {payload_js}" + html[sc:]


html = replace_var(html, 'COHORT_DATA',  COHORT_DATA)
html = replace_var(html, 'COHORT_ROAS',  COHORT_ROAS)
html = replace_var(html, 'SCALING_DATA', SCALING_DATA,
                   sentinels=('/* SCALING_DATA START */', '/* SCALING_DATA END */'))


## Patch DATA.cohort_monthly_funded so the Overall Funnel "Cohort Funded"
## column reconciles with Cohort Speed monthly_cohort funded values.
def patch_cohort_monthly_funded(html, mapping):
    """Surgically replace the cohort_monthly_funded:{...} entry inside the
    DATA const without rewriting the whole (huge) DATA blob."""
    payload = json.dumps(mapping, separators=(',', ':'), ensure_ascii=False)
    pat = re.compile(r'"cohort_monthly_funded"\s*:\s*\{[^{}]*\}')
    new_html, n = pat.subn(f'"cohort_monthly_funded":{payload}', html, count=1)
    if n != 1:
        # If the key isn't present, splice it in just after "m_funnel":
        m = re.search(r'"m_funnel"\s*:\s*\[', html)
        if not m:
            print("  WARN: cohort_monthly_funded key not found and m_funnel not found — skipping patch")
            return html
        # Walk braces from m.start() to find end of m_funnel array
        i = html.find('[', m.start())
        depth = 0
        for k in range(i, len(html)):
            if html[k] == '[': depth += 1
            elif html[k] == ']':
                depth -= 1
                if depth == 0:
                    end_arr = k
                    break
        return html[:end_arr+1] + ',"cohort_monthly_funded":' + payload + html[end_arr+1:]
    return new_html


cohort_monthly_funded = {row['label']: row['funded'] for row in monthly_cohort}
html = patch_cohort_monthly_funded(html, cohort_monthly_funded)
print(f"  Patched DATA.cohort_monthly_funded: {cohort_monthly_funded}")


## Patch DATA.m_funnel total.funded for each month to use the fresh NC CSV
## (only when the NC CSV has Date column). This keeps the Overall Funnel
## "Cumul. Funded" column reconciled with SCALING_DATA.apr_nc_actual.
def patch_m_funnel_funded(html, mapping):
    if not mapping:
        return html
    # Find each m_funnel row and surgically update its total.funded field
    # without rewriting the whole DATA blob. Each row looks like:
    #   {"label":"Apr 2026","core":{...},"exp":{...},"total":{...,"funded":N,...}}
    patched = 0
    for label, new_val in mapping.items():
        # Find the row's "label":"..." marker, then walk forward to its
        # "total":{...} object's "funded":N entry.
        label_pat = '"label":"' + label + '"'
        i = html.find(label_pat)
        if i < 0:
            continue
        # Look for the *next* "total":{ ... "funded":N
        m = re.search(r'"total"\s*:\s*\{[^{}]*?"funded"\s*:\s*(\d+)', html[i:])
        if not m:
            continue
        absolute = i + m.start(1)
        old_str = m.group(1)
        html = html[:absolute] + str(int(new_val)) + html[absolute + len(old_str):]
        patched += 1
    return html, patched


if nc_has_dates:
    nc_label_map = {}
    for mk, count in nc_funded_by_month.items():
        y, m = mk.split('-')
        label = datetime(int(y), int(m), 1).strftime('%b %Y')
        nc_label_map[label] = count
    html, patched_n = patch_m_funnel_funded(html, nc_label_map)
    print(f"  Patched DATA.m_funnel total.funded for {patched_n}/{len(nc_label_map)} months: {nc_label_map}")


## Patch top-level Overall Funnel keys with the freshly-rebuilt data
## (only when we own them — i.e. NC CSV had dates).
def patch_top_level_key(html, key, payload):
    """Replace the value of a top-level key inside the DATA const.
    Works for arrays (...:[...]) and scalars (...:N). Uses brace/bracket
    walking so we don't need a full JS parser.
    """
    payload_js = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)
    needle = f'"{key}":'
    i = html.find(needle)
    if i < 0:
        return html, False
    j = i + len(needle)
    # Skip whitespace
    while j < len(html) and html[j] in ' \t\n':
        j += 1
    open_ch = html[j] if j < len(html) else ''
    if open_ch == '[' or open_ch == '{':
        close_ch = ']' if open_ch == '[' else '}'
        depth = 0
        end = -1
        in_str = False
        esc = False
        for k in range(j, len(html)):
            c = html[k]
            if in_str:
                if esc: esc = False
                elif c == '\\': esc = True
                elif c == '"': in_str = False
                continue
            if c == '"':
                in_str = True
                continue
            if c == open_ch:
                depth += 1
            elif c == close_ch:
                depth -= 1
                if depth == 0:
                    end = k
                    break
        if end < 0: return html, False
        return html[:j] + payload_js + html[end+1:], True
    else:
        # scalar — walk to next "," or "}"
        end = j
        while end < len(html) and html[end] not in ',}':
            end += 1
        return html[:j] + payload_js + html[end:], True


if overall_data_blocks:
    print("  Patching DATA top-level Overall Funnel keys…")
    patched_keys = []
    failed_keys = []
    for k, v in overall_data_blocks.items():
        html, ok = patch_top_level_key(html, k, v)
        (patched_keys if ok else failed_keys).append(k)
    print(f"    Patched: {patched_keys}")
    if failed_keys:
        print(f"    NOT patched (key not found in DATA): {failed_keys}")

pathlib.Path(HTML_PATH).write_text(html)
print(f"  HTML written: {len(html):,} bytes")

target_month_label = datetime(TARGET_YEAR, TARGET_MONTH, 1).strftime('%b %Y')
if TARGET_MONTH == 1:
    baseline_label = f"Jan–Dec {TARGET_YEAR-1}"
else:
    baseline_label = f"Jan–{datetime(TARGET_YEAR, TARGET_MONTH-1, 1).strftime('%b')} {TARGET_YEAR}"
print("\n=== SUMMARY ===")
print(f"Snapshot:          {SNAPSHOT}")
print(f"Target month:      {target_month_label} (day {day_of_month}/{DAYS_IN_TARGET_MONTH}, {days_remaining} remaining)")
print(f"Baseline period:   {baseline_label}")
print(f"Window:            {WINDOW_START} → {WINDOW_END}")
print(f"Cohort users:      {len(users):,}")
print(f"NC campaigns:      {len(nc_spend):,}")
print(f"NC total spend:    ${nc_total_spend:,.0f}")
print(f"Baseline cohort:   {q1_cf:,} (same-month funded)")
print(f"Target-month actual: {apr_cohort_actual:,} (same-month funded)")
print(f"Cohort target:     {cohort_target:,} (from NC target {NC_MONTHLY_TARGET:,} × ratio {target_ratio:.3f})")
print(f"Scaling Decisions: {len(campaigns):,} campaigns")
print(f"Cohort Speed:      {len(daily_cohort)} daily buckets (last: {daily_cohort[-1]['date']})")
print(f"Cohort ROAS:       {[r['label'] for r in cr_rows]}")
