# Jupytext Workflow for iSamples Notebooks

This guide explains how to use **jupytext** to pair `.ipynb` notebooks with `.py` companion files for better git diffing and Claude Code editing.

## The Problem

1. **Git diffs of `.ipynb` files are messy**: Execution outputs, cell metadata, and JSON noise obscure actual code changes
2. **Claude Code file size limits**: Large notebooks with outputs can exceed token limits
3. **Merge conflicts**: Notebooks with outputs create unnecessary conflicts

## The Solution: Jupytext Pairing

**Jupytext** creates a two-file system:
- **`.ipynb`** - Full notebook with outputs (local development, `.gitignore`'d or committed)
- **`.py`** - Clean Python representation (version controlled, diffed, edited)

### Benefits

✅ **For Git**: Diff/review `.py` files with clean code (no output noise)
✅ **For Claude Code**: Edit `.py` files directly (no token limit issues)
✅ **For Humans**: Keep `.ipynb` with outputs for local Jupyter development
✅ **Auto-sync**: Changes to either file automatically update the other

---

## Quick Start

### 1. Pair a Notebook

```bash
# Pair single notebook
~/bin/nb_pair.sh examples/basic/my_notebook.ipynb

# Pair all notebooks in directory
~/bin/nb_pair.sh examples/**/*.ipynb
```

This creates:
- `my_notebook.ipynb` (original, with outputs)
- `my_notebook.py` (new, clean code with `# %%` cell markers)

### 2. Update .gitignore (Usually NOT Needed!)

**Recommended: Commit BOTH files**
```bash
# No .gitignore changes needed
# Commit both .ipynb (with outputs) and .py (clean code)
git add notebook.ipynb notebook.py
```

**Why commit both?**
- ✅ Outputs are valuable (show results without re-running)
- ✅ Reviewers can see both code and results
- ✅ `.py` file provides clean diffs for code review
- ✅ `.ipynb` provides rendered outputs on GitHub
- ✅ Best of both worlds!

**Only ignore .ipynb if:**
- Outputs are huge (>10MB) or contain sensitive data
- Notebooks change frequently with trivial output differences
- CI/CD regenerates outputs automatically

```gitignore
# Only if you have a specific reason:
# *.ipynb
```

### 3. Normal Workflow

**Option A: Edit in Jupyter (recommended)**
```bash
# Edit notebook in Jupyter as usual
jupyter lab

# Jupytext auto-syncs .ipynb ↔ .py on save
# Commit BOTH files
git add examples/basic/my_notebook.ipynb examples/basic/my_notebook.py
git commit -m "Update analysis query"
```

**Option B: Edit .py directly (for Claude Code)**
```bash
# Claude edits the .py file
# Then sync back to .ipynb:
~/bin/nb_pair.sh --sync examples/basic/my_notebook.py

# Or use jupytext directly:
jupytext --sync examples/basic/my_notebook.py
```

---

## Detailed Usage

### Pairing Commands

```bash
# Pair notebook (creates .py companion)
~/bin/nb_pair.sh notebook.ipynb

# Sync after editing either file
~/bin/nb_pair.sh --sync notebook.ipynb

# Remove pairing (keeps .py file)
~/bin/nb_pair.sh --unpair notebook.ipynb
```

### Manual Jupytext Commands

```bash
# Pair with percent format (# %% cell markers)
jupytext --set-formats ipynb,py:percent notebook.ipynb

# Sync changes between files
jupytext --sync notebook.ipynb

# Convert without pairing
jupytext --to py:percent notebook.ipynb
```

### Understanding the .py Format

The `.py` companion uses **percent format**:

```python
# %% [markdown]
# # My Notebook Title
# This is a markdown cell

# %%
import pandas as pd
print("This is a code cell")

# %%
# Another code cell
result = pd.DataFrame({'a': [1, 2, 3]})
```

**Benefits of percent format**:
- Valid Python file (can run with `python notebook.py`)
- Clear cell boundaries (`# %%`)
- Claude Code can edit directly
- Git diffs show actual code changes

---

## Git Configuration (Optional)

Enable better notebook diffs in git:

```bash
# Configure jupytext as diff driver
git config diff.jupytext.command 'jupytext --to md --set-formats - -o -'

# Add to .gitattributes (already done)
echo '*.ipynb diff=jupytext' >> .gitattributes
```

---

## Recommended Workflows

### Workflow 1: Development (Jupyter + Pairing)

1. **Pair notebook**: `~/bin/nb_pair.sh notebook.ipynb`
2. **Edit in Jupyter**: Work normally, outputs saved to `.ipynb`
3. **Commit .py**: Auto-synced on save, commit this file
4. **Review**: Git diffs show clean code changes

**Best for**: Active development with outputs

### Workflow 2: Claude Code Editing

1. **Pair notebook**: `~/bin/nb_pair.sh notebook.ipynb`
2. **Claude edits .py**: No token limits, clean diffs
3. **Sync back**: `~/bin/nb_pair.sh --sync notebook.ipynb`
4. **Run in Jupyter**: Execute to generate outputs

**Best for**: Large refactoring, architecture changes

### Workflow 3: Existing Notebooks

```bash
# Pair all existing notebooks
find examples -name "*.ipynb" -exec ~/bin/nb_pair.sh {} \;

# Add to git
git add examples/**/*.py
git commit -m "Add jupytext pairing for all notebooks"

# Optionally ignore .ipynb files
echo "*.ipynb" >> .gitignore
```

---

## Troubleshooting

### Q: Changes not syncing?

**A**: Manually sync:
```bash
~/bin/nb_pair.sh --sync notebook.ipynb
# or
jupytext --sync notebook.ipynb
```

### Q: Want to stop pairing?

**A**: Unpair and delete .py:
```bash
~/bin/nb_pair.sh --unpair notebook.ipynb
rm notebook.py
```

### Q: Merge conflict in .ipynb?

**A**: Resolve in .py file, then sync:
```bash
# Fix conflict in notebook.py
git add notebook.py
~/bin/nb_pair.sh --sync notebook.ipynb
```

### Q: Claude Code still hits token limits?

**A**: Edit the `.py` file directly:
- Claude reads `notebook.py` (clean, no outputs)
- Makes changes
- Sync: `~/bin/nb_pair.sh --sync notebook.py`
- Run in Jupyter to regenerate outputs

---

## Integration with Existing Tools

### With nb_source_diff.py

Both tools complement each other:
- **nb_source_diff.py**: Diff `.ipynb` files without outputs (one-off)
- **jupytext pairing**: Permanent `.py` companions (ongoing)

Use **jupytext** for:
- Claude Code editing (avoids token limits)
- Normal git workflow (commit .py, diff .py)
- Long-term maintenance

Use **nb_source_diff.py** for:
- Quick diffs of unpaired notebooks
- Legacy notebooks you don't want to pair
- Ad-hoc comparisons

### With Git Hooks (Advanced)

Auto-sync on commit:

```bash
# .git/hooks/pre-commit
#!/bin/bash
for ipynb in $(git diff --cached --name-only | grep '.ipynb$'); do
    jupytext --sync "$ipynb"
    git add "${ipynb%.ipynb}.py"
done
```

---

## Examples in This Repository

### Paired Notebooks (After Setup)

```
examples/basic/
├── oc_parquet_analysis_enhanced.ipynb  (full notebook with outputs)
└── oc_parquet_analysis_enhanced.py      (clean code, version controlled)
```

**Git workflow**:
```bash
# Edit in Jupyter
jupyter lab examples/basic/oc_parquet_analysis_enhanced.ipynb

# Auto-synced to .py on save
# Commit clean code
git add examples/basic/oc_parquet_analysis_enhanced.py
git commit -m "Add geographic classification analysis"

# PR reviewers see clean Python diff, not JSON noise
```

---

## Best Practices

1. **✅ DO**: Pair notebooks you actively develop
2. **✅ DO**: Commit BOTH `.ipynb` and `.py` files (usually!)
3. **✅ DO**: Review `.py` diffs in PRs for code changes
4. **✅ DO**: Check `.ipynb` diffs for output changes
5. **✅ DO**: Let Claude Code edit `.py` files directly
6. **⚠️ CONSIDER**: Ignoring `.ipynb` files if outputs are huge/sensitive
7. **❌ DON'T**: Manually edit both files separately (sync instead)
8. **❌ DON'T**: Commit `.ipynb` AND `.py` with conflicting changes

---

## References

- **Jupytext docs**: https://jupytext.readthedocs.io/
- **Helper script**: `~/bin/nb_pair.sh`
- **Diff tool**: `~/bin/nb_source_diff.py`
- **This guide**: `/Users/raymondyee/C/src/iSamples/isamples-python/JUPYTEXT_WORKFLOW.md`

---

**Last Updated**: 2025-10-15 by Claude Code
