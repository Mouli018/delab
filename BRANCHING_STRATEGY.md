# Git Branching Strategy for Data Engineering Teams

## Overview

This project uses a **GitFlow-Lite** branching model adapted for data engineering workflows.
The key differences from standard GitFlow are:
- **Data quality gates** are enforced at merge time via CI/CD (not just code review).
- **Pipeline artifacts** (CSVs, DB files) are _never_ committed — `.gitignore` excludes them.
- **Configuration-as-code** (pipeline configs, schema definitions) is treated the same as application code.

```
main          ←── hotfix/*
  ↑
develop       ←── feature/*
              ←── fix/*
              ←── data/*
```

---

## Branch Definitions

| Branch | Purpose | Protected? | Direct Push? |
|--------|----------|-----------|--------------|
| `main` | Production-ready, tagged releases | ✅ Yes | ❌ Never |
| `develop` | Integration branch, always deployable | ✅ Yes | ❌ Never |
| `feature/<topic>` | New pipeline features or modules | ❌ No | ✅ Developer |
| `fix/<issue>` | Bug fixes for non-critical issues | ❌ No | ✅ Developer |
| `data/<dataset>` | Data schema or source changes | ❌ No | ✅ Developer |
| `hotfix/<name>` | Urgent production fixes | ❌ No | ✅ Lead only |
| `release/<version>` | Release candidate stabilisation | ❌ No | ✅ Lead only |

---

## Naming Conventions

### Branch Names
```
feature/add-cdc-hashing
feature/week6-streaming-kafka
fix/null-id-drop-bug
data/new-inventory-schema
hotfix/warehouse-load-timeout
release/v1.2.0
```

**Rules:**
- All lowercase, hyphen-separated (`kebab-case`)
- Prefix with type: `feature/`, `fix/`, `data/`, `hotfix/`, `release/`
- Short and descriptive (< 50 characters)

### Commit Messages — Conventional Commits

All commits MUST follow [Conventional Commits v1.0](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <short description>

[optional body]

[optional footer: BREAKING CHANGE / Closes #issue]
```

**Types for data engineering:**

| Type | When to use |
|------|-------------|
| `feat` | New pipeline stage, new data source, new transformation |
| `fix` | Bug fix in transformation logic, load error, schema fix |
| `test` | Adding or updating tests |
| `ci` | CI/CD workflow changes |
| `data` | Schema migrations, seed data changes |
| `docs` | Documentation only |
| `refactor` | Code restructuring without behaviour change |
| `perf` | Performance improvements (e.g., batch size tuning) |
| `chore` | Dependency updates, `.gitignore`, tooling |

**Examples:**
```
feat(etl): add incremental load with high-watermark tracking
fix(transform): strip whitespace from productDisplayName
test(week4): add unit tests for price_estimate derived column
ci: add GitHub Actions workflow for automated pipeline testing
data(schema): add stock_qty column to inventory table
```

---

## Workflow: Feature Development

```bash
# 1. Start from an up-to-date develop branch
git checkout develop
git pull origin develop

# 2. Create your feature branch
git checkout -b feature/my-new-pipeline-stage

# 3. Work, commit often using Conventional Commits
git add week2_etl/pipeline_week2.py tests/test_transformations.py
git commit -m "feat(etl): add CDC row-hashing for change detection"

# 4. Keep your branch up to date with develop
git fetch origin
git rebase origin/develop

# 5. Push and open a Pull Request → develop
git push origin feature/my-new-pipeline-stage
# → Open PR on GitHub: feature/my-new-pipeline-stage → develop
```

---

## Pull Request Rules

### PR into `develop`
- ✅ CI must pass (all unit tests green, compile check clean)
- ✅ At least 1 peer review approval
- ✅ No DB files, CSV outputs, or large binary files included
- ✅ Commit messages follow Conventional Commits

### PR into `main`
- ✅ All PR-into-develop rules apply
- ✅ Integration tests must pass
- ✅ PR title must include version bump: e.g. `release: v1.2.0`
- ✅ `CHANGELOG.md` or release notes updated

---

## Tagging & Versioning

Use **Semantic Versioning** (`MAJOR.MINOR.PATCH`):

```bash
# After merging release branch into main
git checkout main
git tag -a v1.0.0 -m "release: initial production pipeline"
git push origin v1.0.0
```

| Increment | Trigger |
|-----------|---------|
| MAJOR | Breaking schema change, incompatible API change |
| MINOR | New pipeline stage, new data source, new feature |
| PATCH | Bug fix, performance improvement, documentation |

---

## Data Engineering–Specific Rules

### What to ALWAYS commit
```
✅ Pipeline source code          (*.py)
✅ SQL schema definitions        (*.sql)
✅ Configuration files           (*.yaml, *.json config)
✅ Tests                         (tests/*.py)
✅ CI/CD workflow definitions    (.github/workflows/*.yml)
✅ Documentation                 (*.md)
✅ requirements.txt / pyproject.toml
```

### What to NEVER commit
```
❌ Database files                (*.db, *.duckdb)
❌ Large raw CSV/Parquet files   (data/fashion-dataset/, week*/outputs/*.csv)
❌ Pipeline output artifacts     (week*/outputs/)
❌ API keys / secrets            (.env files, credentials)
❌ Virtual environments          (venv/)
❌ Jupyter notebook outputs      (*.ipynb with cell outputs)
```

> All excluded files are listed in `.gitignore`. Run `git status` before every commit to confirm no artifacts are staged.

---

## CI/CD Integration

Every push triggers the GitHub Actions pipeline (`.github/workflows/ci.yml`):

```
push / PR
    │
    ├── lint          → flake8 style check
    ├── unit-tests    → fast pure unit tests (< 30s)
    ├── integration-tests → full pytest suite (skips if artifacts missing)
    └── compile-check → py_compile all .py files
```

**Merge is blocked if any job fails.** This is the data quality gate.

---

## Quick Reference Card

```bash
# New feature
git checkout develop && git pull origin develop
git checkout -b feature/<topic>
# ... work ...
git commit -m "feat(<scope>): <description>"
git push origin feature/<topic>
# Open PR → develop

# Hotfix (production bug)
git checkout main && git pull origin main
git checkout -b hotfix/<name>
# ... fix ...
git commit -m "fix(<scope>): <description>"
# Open PR → main AND → develop

# Release
git checkout develop && git pull origin develop
git checkout -b release/vX.Y.Z
# ... stabilise, bump version ...
git commit -m "chore: bump version to vX.Y.Z"
# Open PR → main, tag on merge
```
