# Quick Reference: Notebook Workflows

## Two Tools for Different Needs

### 1. **nb_source_diff.py** - Quick Diffs Without Outputs
```bash
# One-off diff of any notebook vs git history
nb-diff notebook.ipynb HEAD
nb-diff notebook.ipynb HEAD~5
```
**Use when**: Quick comparison, unpaired notebooks, legacy files

---

### 2. **jupytext pairing** - Permanent .py Companions
```bash
# Pair notebook (creates .py file)
~/bin/nb_pair.sh notebook.ipynb

# Sync after editing
~/bin/nb_pair.sh --sync notebook.ipynb
```
**Use when**: Active development, Claude Code editing, clean git workflow

---

## Decision Tree

```
Need to diff a notebook?
├─ One-time comparison → nb-diff
└─ Ongoing development → jupytext pair

Claude Code hitting token limits?
└─ Pair with jupytext, edit .py file

Want clean git diffs?
├─ Quick → nb-diff
└─ Permanent → jupytext pair + commit .py

Collaborating on notebooks?
└─ Pair all notebooks, commit .py files
```

---

## Setup New Notebook (Recommended)

```bash
# 1. Create notebook in Jupyter
jupyter lab

# 2. Pair immediately
~/bin/nb_pair.sh examples/basic/my_analysis.ipynb

# 3. Add to git
git add examples/basic/my_analysis.py

# 4. Develop normally - changes auto-sync
```

---

## Quick Commands Cheat Sheet

```bash
# DIFF TOOLS
nb-diff notebook.ipynb              # vs HEAD
nb-diff notebook.ipynb HEAD~3       # vs 3 commits ago

# PAIRING
~/bin/nb_pair.sh notebook.ipynb     # Pair (create .py)
~/bin/nb_pair.sh --sync notebook.ipynb  # Sync changes
~/bin/nb_pair.sh examples/**/*.ipynb    # Pair all

# JUPYTEXT DIRECT
jupytext --set-formats ipynb,py:percent notebook.ipynb  # Pair
jupytext --sync notebook.ipynb                          # Sync
```

---

## Claude Code Editing Workflow

### Problem: Large notebook with outputs exceeds token limits

### Solution: Edit .py companion

```bash
# 1. Pair notebook (if not already paired)
~/bin/nb_pair.sh notebook.ipynb

# 2. Tell Claude to edit: notebook.py (NOT .ipynb)
# Claude edits clean .py file without output noise

# 3. Sync back to notebook
~/bin/nb_pair.sh --sync notebook.ipynb

# 4. Run in Jupyter to regenerate outputs
jupyter lab
```

---

## Git Workflow with Pairing

```bash
# Development
jupyter lab my_notebook.ipynb      # Edit in Jupyter
# Changes auto-sync to my_notebook.py on save

# Commit BOTH files (recommended!)
git status                          # Shows both files changed
git diff my_notebook.py            # Review code changes (clean!)
git diff my_notebook.ipynb         # Review output changes (optional)
git add my_notebook.ipynb my_notebook.py
git commit -m "Add new analysis"

# PR Review
# Reviewers can:
# - Check .py for code changes (clean diffs)
# - Check .ipynb for output changes (rendered on GitHub)
# - Best of both worlds!
```

---

## Migration: Existing Notebooks

```bash
# Pair all notebooks in project
find examples -name "*.ipynb" -exec ~/bin/nb_pair.sh {} \;

# Commit BOTH .ipynb and .py files (recommended)
git add examples/**/*.ipynb examples/**/*.py
git commit -m "Add jupytext pairing to all notebooks"

# Or if outputs are problematic (less common):
# git add examples/**/*.py
# echo "*.ipynb" >> .gitignore
# git commit -m "Add .py companions, ignore .ipynb"
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Changes not syncing | `~/bin/nb_pair.sh --sync notebook.ipynb` |
| Claude hits token limit | Edit `notebook.py` instead of `notebook.ipynb` |
| Git diff too noisy | Use `nb-diff` or pair with jupytext |
| Want to stop pairing | `~/bin/nb_pair.sh --unpair notebook.ipynb` |
| Merge conflict | Resolve in `.py`, then `--sync` |

---

## Files & Docs

- **Helper script**: `~/bin/nb_pair.sh`
- **Diff tool**: `~/bin/nb_source_diff.py`
- **Full guide**: `JUPYTEXT_WORKFLOW.md`
- **This quickref**: `QUICKREF_NOTEBOOKS.md`

---

**Last Updated**: 2025-10-15
