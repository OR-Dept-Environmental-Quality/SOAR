# Architecture Decision Records (Draft Bundle)

> **Note:** In the repository, split each ADR below into its own file named `ADR-XXX-<slug>.md` and keep them in the `docs/adr/` folder. Cross‑link them from the README under an *Architecture Decisions* section. Follow the numbering sequence strictly—once published, the file numbers must never change.

---

## ADR‑001 – Adopt Architecture Decision Records (ADR) Methodology

**Status**: *Accepted – 2025‑05‑25*

### Context

We need a lightweight, version‑controlled way to capture the “why” behind key technical choices so that future maintainers (internal or external) can understand and evolve the system without guess‑work.

### Decision

- Adopt the Markdown‑based *Architecture Decision Record* format popularised by Michael Nygard (see [template](https://github.com/joelparkerhenderson/architecture-decision-record)).
- Store each ADR in `docs/adr/ADR‑NNN‑<slug>.md` with incremental numbering.
- Use the sections **Context / Decision / Consequences / References**.
- Status keywords: *Proposed*, *Accepted*, *Superseded*, *Deprecated*, *Rejected*.

### Consequences

- Project history stays discoverable via Git.
- New contributors can align with existing decisions instead of re‑litigating them.

### References

- Michael Nygard, *Lightweight Architecture Decision Records* (2011)
- joelparkerhenderson/architecture‑decision‑record GitHub repo

---

## ADR‑002 – Data‑Lake Layering Strategy (Raw → Transform → Stage)

**Status**: *Accepted – 2025‑05‑25*

### Context

The client explicitly requires a three‑layer storage pattern, and we want to align with the industry‑standard Medallion/Lakehouse approach (Bronze/Silver/Gold) for clarity and scalability.

### Decision

- Map layers as follows:
  - **Raw (Bronze)** – untouched outputs from source APIs stored as JSON (AQS) or CSV (Envista). Immutable, append‑only.
  - **Transform (Silver)** – cleaned, standardised, unit‑converted data stored as columnar Parquet files partitioned by pollutant and year.
  - **Stage (Gold)** – Power‑BI‑optimised tidy CSVs plus helper tables (monitor coverage, AQI daily) partitioned by pollutant and year.
- Folder naming: `<layer>/<source>/<pollutant>/<YYYY>/…`.

### Consequences

- Clear contract between steps enables independent unit‑testing and parallelism.
- Stage files stay small enough (< 250 MB) for fast Power BI refresh.

### References

- Databricks blog, *What is a Medallion Architecture?* (2022)

---

## ADR‑003 – Language Choice: Python 3.12 for ETL Pipelines

**Status**: *Accepted – 2025‑05‑25*

### Context

The legacy code is in R but the client wants a Python‑centric, modular, easily‑deployable solution on Windows.

### Decision

- Rewrite all ETL logic in Python 3.12 using only widely‑supported libraries (requests, pandas, pyarrow, geopandas, python‑dotenv).
- Provide a `requirements.txt` to pin exact versions.
- Keep the original R scripts in `reference/` for traceability; they are not executed.

### Consequences

- Easier hiring and maintenance (larger Python talent pool).
- Higher performance for I/O‑bound tasks due to async options (future).

---

## ADR‑004 – File Formats & Partitioning

**Status**: *Accepted – 2025‑05‑25*

### Context

Power BI handles CSV well for final consumption, but intermediate transforms benefit from columnar formats.

### Decision

- **Raw**: keep original format (JSON or CSV) to preserve fidelity.
- **Transform**: store as Snappy‑compressed Parquet.
- **Stage**: write UTF‑8 CSV with ISO‑8601 timestamps.
- Partition Prq/CSV by `pollutant/year` to cap folder size and enable pruning.

### Consequences

- 8× faster load for Transform layer; Stage remains instantly queryable by Power BI’s folder connector.

---

## ADR‑005 – Authoritative Source & Fallback Logic

**Status**: *Accepted – 2025‑05‑25*

### Context

EPA AQS is the gold standard but releases certified data with delay. Envista provides near‑realtime hourly values plus extra metadata.

### Decision

- For each hour, prefer AQS sample values when available; otherwise pull Envista.
- Keep *both* sources in Raw for auditability.
- In Transform, when deduplicating, persist the chosen source in a `data_source` column.

### Consequences

- Continuous data stream even during AQS gaps; provenance remains transparent.

---

## ADR‑006 – Envista Qualifier → Simplified Qual Mapping

**Status**: *Proposed – 2025‑05‑25*

### Context

Envista qualifier codes are verbose and source‑specific; Power BI users need a compact set similar to AQS qualifiers (e.g., `V` = *valid*, `I` = *invalid*, `M` = *missing*).

### Decision

| Envista Code | Meaning                | Simple Qual |
| ------------ | ---------------------- | ----------- |
| 0            | Valid                  | V           |
| 803          | Instrument calibration | I           |
| 998          | Missing data           | M           |
| …            | …                      | …           |

The mapping YAML will live in `config/envista_quality_map.yml` and be applied during Transform.

### Consequences

- Unified semantics across AQS & Envista streams.
- Future Envista code additions require only a YAML update, not code change.

---

## ADR‑007 – Orchestration: Windows Task Scheduler

**Status**: *Accepted – 2025‑05‑25*

### Context

The client’s deployment target is a Windows workstation with no container runtime.

### Decision

- Provide a signed `register_task.ps1` that imports a `.xml` task definition (daily 02:00 local).
- The task runs `python main.py --schedule daily` in a dedicated service account with minimal privileges.

### Consequences

- Zero external dependencies; standard Windows tooling.
- Must monitor exits codes via email alert script; Task Scheduler’s own logging is not sufficient.

---

## ADR‑008 – Logging & Monitoring

**Status**: *Accepted – 2025‑05‑25*

### Context

Silent failures are a top risk for unattended ETL.

### Decision

- Adopt the [`loguru`](https://github.com/Delgan/loguru) library as the project‑standard logger instead of the stdlib `logging` module because it offers easier configuration, structured JSON output and built‑in rotation.
- Configure two sinks at application start‑up:
  - **File sink** `logs/pipeline.log` with `rotation="10 MB"` and `retention=7`, `compression="zip"`.
  - **Stderr sink** for interactive runs, with minimum level `WARNING`.
- Enable `serialize=True` on both sinks so each line is emitted as a JSON object: `{"time": "...", "level": "...", "name": "...", "message": "..."}`.
- All library modules call `from loguru import logger` and log with `logger.info/debug/warning(...)`; no child loggers or manual handlers are required.

### Consequences

- Configuration is reduced to \~5 lines and can live in `scripts/run_pipeline.py`.
- JSON lines are immediately parseable by Power BI or any SIEM without custom formatters.
- Built‑in rotation plus compression prevents uncontrolled log growth without external utilities.

---

## README Cross‑Links (snippet)

```markdown
## 🗂 Architecture Decisions
This project uses [ADR](https://adr.github.io/) to capture major technical choices:

| ID | Purpose |
|----|---------|
| ADR‑001 | Adopt ADR methodology |
| ADR‑002 | Data‑Lake Layering Strategy |
| ADR‑003 | Language Choice: Python |
| ADR‑004 | File Formats & Partitioning |
| ADR‑005 | Authoritative Source & Fallback Logic |
| ADR‑006 | Envista Qualifier Mapping |
| ADR‑007 | Orchestration: Windows Task Scheduler |
| ADR‑008 | Logging & Monitoring |
```

---

> **Next step:** move each ADR to its own file, mark ADR‑006 as *Accepted* once the client approves the exact qualifier mapping list.

