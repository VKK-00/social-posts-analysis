# Research Access Guide

How to get full-fidelity data from each platform **legally** for scientific
research, and where to plug the credentials into this pipeline.

> This project intentionally does **not** include CAPTCHA solving,
> fingerprint-spoofing (undetected-chromedriver-style) tooling, or
> authentication-wall circumvention. Those techniques violate every major
> platform's terms of service and can expose researchers and institutions to
> legal liability — and data obtained that way is typically unpublishable.
> Every major platform instead offers a sanctioned research access program,
> usually free for verified academics. Use those.

## Quick map

| Platform | Sanctioned route | Where it plugs in here |
|---|---|---|
| X (Twitter) | **X Academic API** | Already wired: `mode: x_api` + `X_BEARER_TOKEN` |
| Facebook Pages you manage | **Meta Graph API** | Already wired: `mode: api` + `META_ACCESS_TOKEN` |
| Instagram professional account | **Instagram Graph API** | Already wired: `mode: instagram_graph_api` + `INSTAGRAM_ACCESS_TOKEN` |
| Threads (owned account) | **Threads API** | Already wired: `mode: threads_api` + `THREADS_ACCESS_TOKEN` |
| Telegram public channels/groups | **MTProto with your own account** | Already wired: `mode: mtproto` |
| Facebook/Instagram public content at scale | **Meta Content Library** | External tool; see below |

## Platform details

### X (Twitter) — Academic API

1. Apply at <https://developer.x.com> for the Academic Research track
   (affiliation verification required; free tier also works for small pilots).
2. Export your Bearer Token.
3. Configure:

```yaml
source: {platform: "x", source_name: "account_handle"}
collector:
  mode: "x_api"
  x_api: {enabled: true, page_size: 100}
```

```powershell
$env:X_BEARER_TOKEN = "your-academic-token"
social-posts-analysis run-all --config config/project.local.yaml
```

The Academic tier lifts the severe rate/search limits that make public web
scraping of replies pointless — replies, quote context, and historical search
become complete instead of best-effort.

### Facebook / Instagram — two legitimate tracks

**Owned assets (Pages / professional accounts):** the Meta Graph API path is
fully wired (`META_ACCESS_TOKEN`, `INSTAGRAM_ACCESS_TOKEN`). This is the right
tool when you study a page/account you administer or have granted access to.

**Public content across the platform:** apply to the
[Meta Content Library](https://transparency.meta.com/researchtools/meta-content-library)
(successor to CrowdTangle). It is a separate web UI + API for approved
nonprofit/academic researchers studying public discourse. Approval requires an
institutional affiliation and a described research purpose; EU researchers can
also invoke DSA Article 40 researcher data access.

Integration pattern for Content Library exports: place exported JSON/CSV
datasets under `data/raw/<run_id>/` alongside a manifest-shaped file, or load
them into DuckDB next to the parquet tables — all downstream analysis
(cascades, near-duplicates, stance, reports) operates on the processed tables
and does not care whether rows came from a collector or an official export.

### Telegram

Public channels and groups are fully collectible through MTProto with your
own user account (`TELEGRAM_SESSION_FILE` / `TELEGRAM_API_ID` /
`TELEGRAM_API_HASH`) — this is ordinary client usage of the official protocol,
not a bypass. No special research program is required.

### Threads / other Meta surfaces

Threads API covers owned-account scenarios (`THREADS_ACCESS_TOKEN`). Broader
public Threads research currently has no sanctioned bulk channel — document
this as a coverage limitation rather than scraping around it.

## Compliance checklist before you publish

1. **Terms of service**: cite the sanctioned access method used; avoid
   claiming coverage obtained through ToS-violating means.
2. **GDPR**: personal data of commenters falls under GDPR. Use
   `normalization.pseudonymize_authors: true` before sharing any dataset,
   state the lawful basis, and minimise retention.
3. **DSA Art. 40 (EU)**: vetted researchers can compel platforms for data
   access to study systemic risks — the strongest legal basis available.
4. **IRB / ethics review**: human-subject considerations usually apply to
   social-media research even when data is public.
5. **Reproducibility**: record `analysis_runs` provenance, keep
   `collection_runs.parquet` warnings (they document coverage gaps honestly),
   and prefer deterministic providers (`hash` embeddings, `langdetect`) when
   reviewers must reproduce results bit-for-bit.

## What this pipeline will never do

- Solve CAPTCHAs or security challenges.
- Spoof browser fingerprints to defeat bot detection.
- Access content behind authentication walls using anything other than a
  session you are legitimately entitled to use.
- Circumvent rate limits by disguising automated traffic as organic.

These boundaries protect you: data gathered through sanctioned channels is
defensible in peer review, in institutional review, and in court.
