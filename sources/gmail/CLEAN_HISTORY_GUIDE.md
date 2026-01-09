# Guide: Clean Git History of Sensitive Data

## Current Situation

The demo email address `lakeflow.mail.demo@gmail.com` appears in 5 files across your commits.
The password was NEVER committed (good!), but we should remove the email address from history.

## Option 1: Reset and Recommit (Safest - Recommended)

This approach creates a completely clean history without any trace of sensitive data.

### Step 1: Backup Current Work

```bash
# Create backup branch
git branch backup-original-work

# Verify backup
git log backup-original-work --oneline
```

### Step 2: Reset to Before Demo Commits

```bash
# Find the commit before demo scripts were added
git log --oneline

# Reset to the commit before: bcb5578 (the demo scripts commit)
# That would be: 23a6a34 (OAuth helper script)
git reset --soft 23a6a34
```

### Step 3: Clean the Files

```bash
# Remove email from demo scripts
sed -i '' 's/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g' sources/gmail/demo/populate_demo_data.py
sed -i '' 's/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g' sources/gmail/demo/add_incremental_data.py
sed -i '' 's/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g' sources/gmail/demo/README.md
sed -i '' 's/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g' sources/gmail/CONTRIBUTING_GUIDE.md

# Verify changes
grep -r "DEMO_EMAIL_PLACEHOLDER" sources/gmail/ --include="*.py" --include="*.md"
```

### Step 4: Recommit Cleanly

```bash
# Stage demo files
git add sources/gmail/demo/

# Commit demo scripts (clean)
git commit -m "feat(gmail): add demo data generation scripts for evaluation

Add comprehensive demo data scripts to populate test Gmail account:

populate_demo_data.py:
- Creates ~20 realistic emails demonstrating connector capabilities
- Customer support threads, sales communications, system notifications
- Marketing emails and draft messages
- Custom Gmail labels for organization

add_incremental_data.py:
- Adds new emails for CDC demonstration
- Shows historyId cursor-based incremental sync
- Demonstrates efficiency of Gmail History API

demo/README.md:
- Complete setup instructions for demo environment
- Sample SQL queries for Databricks
- Troubleshooting guide

Note: Demo account email configured via placeholder.
Actual credentials stored in internal Keeper vault.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

# Stage merged file
git add sources/gmail/_generated_gmail_python_source.py

# Commit merged file
git commit -m "build(gmail): generate merged connector file for SDP deployment

Generate single-file deployment artifact for Spark Declarative Pipeline:

Generated File:
- _generated_gmail_python_source.py
- Combines libs/utils.py + gmail.py + pipeline/lakeflow_python_source.py
- Self-contained deployable connector

Purpose:
- Temporary workaround for SDP deployment limitations
- Required for current Databricks community connector framework

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"

# Stage documentation
git add sources/gmail/DEMO_ACCESS.md sources/gmail/PULL_REQUEST_TEMPLATE.md sources/gmail/SUBMISSION_CHECKLIST.md sources/gmail/CONTRIBUTING_GUIDE.md

# Commit documentation
git commit -m "docs(gmail): add evaluation and testing documentation

Add comprehensive documentation for evaluators and testing:

DEMO_ACCESS.md:
- Testing guide for evaluators using their own Gmail accounts
- OAuth setup walkthrough (10-15 minutes)
- Read-only scope explanation (gmail.readonly)
- Security best practices and privacy considerations
- Demo account access via internal Keeper vault

PULL_REQUEST_TEMPLATE.md:
- Complete PR description for hackathon submission
- Implementation details and use cases
- Alignment with judging criteria
- Test results and validation

SUBMISSION_CHECKLIST.md:
- Step-by-step submission guide
- Git workflow and best practices
- Files summary and judging criteria alignment

CONTRIBUTING_GUIDE.md:
- Fork-based contribution workflow
- PR creation and management
- Handling feedback and updates

All documentation follows security best practices:
- No public exposure of credentials
- Keeper vault references for internal access
- Clear testing instructions for external users

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

### Step 5: Verify Clean History

```bash
# Check commit history
git log --oneline

# Search for sensitive data (should find none in commits)
git log --all --full-history -S "lakeflow.mail.demo" --source --pretty=format:'%H %s'

# Check current files (should only show placeholder)
grep -r "lakeflow.mail.demo" sources/gmail/ 2>/dev/null || echo "No sensitive data found!"
grep -r "DEMO_EMAIL_PLACEHOLDER" sources/gmail/ 2>/dev/null
```

### Step 6: Clean Up

```bash
# Remove backup if everything looks good
git branch -D backup-original-work

# Or keep it just in case
git branch -m backup-original-work backup-before-cleanup-$(date +%Y%m%d)
```

---

## Option 2: Git Filter-Branch (More Complex)

If you've already pushed to your fork and need to rewrite published history:

### Step 1: Backup

```bash
git branch backup-before-filter
```

### Step 2: Filter History

```bash
git filter-branch --tree-filter '
  if [ -f sources/gmail/demo/populate_demo_data.py ]; then
    sed -i.bak "s/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g" sources/gmail/demo/populate_demo_data.py 2>/dev/null
    rm -f sources/gmail/demo/populate_demo_data.py.bak
  fi
  if [ -f sources/gmail/demo/add_incremental_data.py ]; then
    sed -i.bak "s/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g" sources/gmail/demo/add_incremental_data.py 2>/dev/null
    rm -f sources/gmail/demo/add_incremental_data.py.bak
  fi
  if [ -f sources/gmail/demo/README.md ]; then
    sed -i.bak "s/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g" sources/gmail/demo/README.md 2>/dev/null
    rm -f sources/gmail/demo/README.md.bak
  fi
  if [ -f sources/gmail/CONTRIBUTING_GUIDE.md ]; then
    sed -i.bak "s/lakeflow\.mail\.demo@gmail\.com/DEMO_EMAIL_PLACEHOLDER/g" sources/gmail/CONTRIBUTING_GUIDE.md 2>/dev/null
    rm -f sources/gmail/CONTRIBUTING_GUIDE.md.bak
  fi
' --prune-empty -- feature/gmail-connector-hackathon-2026

# Clean up filter-branch refs
git for-each-ref --format="%(refname)" refs/original/ | xargs -n 1 git update-ref -d
```

### Step 3: Force Push (if already pushed)

```bash
# Only if you've already pushed to your fork
git push fork feature/gmail-connector-hackathon-2026 --force-with-lease
```

---

## Option 3: BFG Repo-Cleaner (Professional Tool)

For larger cleanups, BFG is faster than filter-branch:

### Install BFG

```bash
brew install bfg  # macOS
# or download from: https://rtyley.github.io/bfg-repo-cleaner/
```

### Create Replacement File

```bash
cat > replacements.txt << 'EOF'
lakeflow.mail.demo@gmail.com==>DEMO_EMAIL_PLACEHOLDER
EOF
```

### Run BFG

```bash
bfg --replace-text replacements.txt
```

---

## What To Use

**Recommended**: **Option 1** (Reset and Recommit)
- Cleanest approach
- Easiest to understand and verify
- No complex commands
- Creates perfect history from scratch

**Use Option 2 if**:
- You've already pushed to fork
- Want to keep exact commit timestamps
- Need to preserve other aspects of commits

**Use Option 3 if**:
- Dealing with many files
- Large repository
- Need professional-grade cleaning

---

## After Cleaning

### Verify No Sensitive Data

```bash
# Check commit messages
git log --all --oneline | grep -i "lakeflow.mail"

# Check commit content
git log --all -S "lakeflow.mail.demo@gmail.com" --source --oneline

# Check current working directory
grep -r "lakeflow.mail.demo@gmail.com" sources/gmail/ 2>/dev/null

# Should see placeholders only
grep -r "DEMO_EMAIL_PLACEHOLDER" sources/gmail/
```

### Update References in Code

Create a comment explaining the placeholder:

```python
# Demo account email - actual credentials stored in Keeper vault
# Path: Hackathon 2026 / Gmail Connector Demo Account
DEMO_EMAIL = "DEMO_EMAIL_PLACEHOLDER"  # Replace with your test email
```

---

## Important Notes

1. **Never Use Real Credentials in Code**
   - Use environment variables
   - Use configuration files (.gitignore'd)
   - Use secret management systems (Keeper, AWS Secrets Manager, etc.)

2. **Check Before Committing**
   ```bash
   git diff --cached | grep -i "password\|token\|secret\|credential"
   ```

3. **Use Pre-commit Hooks**
   Create `.git/hooks/pre-commit`:
   ```bash
   #!/bin/bash
   if git diff --cached | grep -q "lakeflow.mail.demo@gmail.com"; then
     echo "ERROR: Demo email found in staged files!"
     exit 1
   fi
   ```

4. **After History Rewrite**
   - All commit SHAs will change
   - Anyone who cloned needs to re-clone
   - Force push will be required
   - Notify collaborators

---

## Final Checklist

- [ ] Backup created
- [ ] History rewritten
- [ ] No sensitive data in commits (verified with git log -S)
- [ ] No sensitive data in files (verified with grep)
- [ ] Placeholders added with comments
- [ ] Documentation updated
- [ ] Ready to push clean history

---

**Recommendation**: Use Option 1 (Reset and Recommit) - it's the safest and cleanest approach for your situation.
