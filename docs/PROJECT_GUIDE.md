# Vote-data ingestion and outlier-week detection

## What the source implements
An Equal Experts data-engineering exercise: ingest vote records into DuckDB and flag unusual weekly vote counts relative to each year's mean.

**Technology:** Python 3.11+, Poetry, DuckDB, Typer, pytest, Docker.

## Code/data inventory
| File | Responsibility |
|---|---|
| `equalexperts_dataeng_exercise/ingest.py` | DuckDB table creation and vote-data ingestion |
| `equalexperts_dataeng_exercise/outliers.py` | Weekly aggregation and outlier calculation |
| `equalexperts_dataeng_exercise/scripts/exercise.py` | Typer CLI for ingest-data, detect-outliers and tests |
| `tests/` | Unit/integration exercises and fixtures |
| `Dockerfile` | Container environment |
| `pyproject.toml` | Poetry dependencies and entry point |

## Architecture
~~~mermaid
flowchart LR
 User[Notebook / CLI user] --> Entry[Typer CLI and Python modules]
 Entry --> Logic[Ingestion and outlier SQL]
 Logic --> External[DuckDB and JSONL]
~~~

## Request / processing sequence
~~~mermaid
sequenceDiagram
 participant User
 participant Program
 participant Dependency
 User->>Program: Run selected workflow
 Program->>Dependency: Load data or call library/service
 Dependency-->>Program: Values / response / error
 Program-->>User: Display result
~~~

## Installation and setup
1. Install Docker or Python 3.11 with Poetry; the repository README contains original container commands.
2. Use poetry install to install declared dependencies.
3. From the repository root run poetry run exercise --help; review available subcommands.
4. Use sample/local JSONL vote data for ingestion, then poetry run exercise detect-outliers; avoid production datasets.
5. Run poetry run exercise test or pytest for available unit coverage and verify outputs on disposable data.

~~~mermaid
flowchart TD
 Clone[Clone or open repository] --> Install[Select Python environment]
 Install --> Dependencies[Review and install required packages]
 Dependencies --> Data[Provide local input / credentials if required]
 Data --> Execute[Run selected example]
 Execute --> Check{Expected outcome?}
 Check -->|Yes| Test[Review tests and output]
 Check -->|No| Fix[Diagnose versions and input]
 Fix --> Dependencies
~~~

## Program behavior
The ingest module creates a DuckDB table for vote IDs, post IDs, vote type IDs and timestamps. The outliers SQL groups by year/week and marks weeks with absolute deviation from yearly average weekly counts > 0.2 (20%).

## Validation and reproducibility
- GitHub Actions validates the README's guide link, Markdown fences and three Mermaid diagrams.
- CI compiles Python source and runs the available pytest suite after dependency installation; see PR check results.
- End-to-end usage must be checked using the setup steps above; do not imply remote APIs or datasets are available by default.

## Limitations and further work
- The original README includes conflict-marker-like text and reviewer-oriented boilerplate; this PR preserves it and adds a separate source-grounded guide.
- Exercise results depend on input files and DuckDB version; do not claim success without tests.
- Project declares a proprietary license; respect exercise rights and data handling.
- No actual geographic outputs or performance statistics are committed, so neither fabricated maps nor invented performance charts belong in this guide.
- Application logic and dependencies are left unchanged by this documentation-only PR.
