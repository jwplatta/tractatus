# Schwab SPXW Open Interest Update Timing

**Date:** 2026-10-19  
**Script:** `scripts/research_oi_update_timing.py`

## Summary

Open interest (OI) in Schwab's SPXW option chain samples is **completely static intraday** and **updates overnight**, with the new value present in the first sample of the following trading day.

## Method

- Source: `options/schwab/` parquets in `tickrake-historical` MinIO bucket (199 trading days, Dec 2025 – Oct 2026). Note: `options/massive/` parquets have OI stripped (all NULL) and cannot be used for this analysis.
- Selected 4 consecutive-day pairs spread across the date range.
- For each pair, picked a near-expiry SPXW expiration (1–5 days out) present in both parquets.
- Focused on near-ATM strikes (within 3% of underlying) — highest gamma, most OI activity.
- Checked: (1) intraday OI stability, (2) last snap day N vs first snap day N+1.

## Results

### OI is frozen throughout the trading session

Zero OI changes detected across all contracts, all snapshots, on every study date. The value at 09:30 is identical to the value at 16:05.

### OI updates are reflected in the first sample of the next morning

| Day boundary | Last snap day N (ET) | First snap day N+1 (ET) | OI updated? |
|---|---|---|---|
| 2025-12-18 → 12-19 | 16:13 | 09:47 | Yes |
| 2026-03-17 → 03-18 | 16:00 | 10:08 | Yes |
| 2026-05-21 → 05-22 | 16:05 | **09:30** | Yes |
| 2026-07-31 → 08-03 | 16:05 | **09:30** | Yes |

The Dec and Mar cases had sparse intraday sampling (7–8 snaps/day), so the first available snap was mid-morning — the actual update boundary is unknown for those dates but still preceded the first sample. The May and Jul cases had minute-level sampling; OI was already updated at the 09:30 open snap, confirming the update arrives **before market open**.

### Example deltas (2026-05-22, near-ATM SPXW calls, expiry 2026-05-22)

| Symbol | Strike | Type | OI day N (EOD) | OI day N+1 (open) | Delta |
|---|---|---|---|---|---|
| SPXW260522C07640000 | 7640 | CALL | 778 | 13,675 | +12,897 |
| SPXW260522C07560000 | 7560 | CALL | 1,963 | 14,221 | +12,258 |
| SPXW260522C07575000 | 7575 | CALL | 2,206 | 12,562 | +10,356 |
| SPXW260522P07360000 | 7360 | PUT  | 1,037 | 7,439  | +6,402  |

## Conclusion

OI in Schwab samples reflects **OCC end-of-day settlement**, cleared overnight and published before the next market open. For any signal or analysis that depends on OI, treat each day's OI as a single daily value — the first snapshot of the day is as good as any other.
