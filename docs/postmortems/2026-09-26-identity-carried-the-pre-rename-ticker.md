# An identity carried the pre-rename ticker, or none

**Rule:** A security's identity carries today's ticker across its whole
history, because bronze is keyed by it. A successor registrant is joined to its
predecessor's CIK only through a table entry that cites the filing. Which
claim is latest never decides a security's ticker.

**Incident / measurement:** host **macmini**, measured 2026-09-26.

After the 2026-09-24 identity repair, the sp500 PIT republish still failed on
two identity gaps, and apex's membership route, still on the pre-repair PIT
revisions, listed dead tickers (issue #158, reported by argon):

- **XOM had no identity after 2020-08-30.** The research put ExxonMobil
  Holdings Corp's CIK (0002115436, successor registrant since 2026-07-01,
  8-K12B 0001193125-26-291990) on sp500's 1996 add, and Exxon Mobil Corp's own
  CIK (0000034088) on djia's. R1 joins by raw CIK, so these were two securities
  and the sp500 claim was capped where djia's began.
- **TRV had no identity from 1997-03-16 to 2009-06-08.** St Paul (TRV today)
  and Travelers Group (C since 1998) were both researched as TRV. R2 had
  stretched Travelers Group's TRV claims to 2009, so St Paul's claim was capped.
- **The current ticker was the old one** for Baker Hughes (BHGE), Revvity
  (PKI), Globe Life (TMK), DuPont (DWDP) and AT&T (SBC). Each Massive probe
  taken on a rename day returned the old ticker as an open claim (BHGE
  2019-10-18, PKI 2023-05-16, TMK 2019-08-08, SBC 2005-11-21), and R1 moved it
  onto the canonical id. PKI, TMK and BHGE have no bronze. SBC's bronze is a
  different company, listed 2022.

The first fix planned ("an extension ends at the next claim with another
symbol") would have fixed none of these: the stale claim is the latest one.
The first R5 draft gave the old GM (Motors Liquidation) the ticker GM, which
is General Motors Company's today; the scratch run caught it.

**Cost:** sp500 PIT could not be republished after the 2026-09-24 repair.
apex served null symbols, dead tickers, and a ticker that is another company's
bronze.

→ test: `tests/test_membership_repair_identity.py::test_a_successor_registrant_cik_is_one_security_with_its_predecessor`,
`::test_r5_a_renamed_security_carries_todays_ticker_not_the_rename_day_probe`,
`::test_r5_frees_todays_ticker_for_the_security_that_holds_it_now`,
`::test_r5_never_hands_a_security_a_ticker_another_holds_today` · spec
`docs/superpowers/specs/2026-09-23-membership-identity-continuity-design.md` §2 R1, R5
