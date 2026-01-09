# How to Submit Your Gmail Connector via Pull Request

## Overview

This guide walks you through the **fork-based contribution workflow**, which is the standard best practice for contributing to open-source repositories where you don't have direct write access.

## Why Fork-Based Workflow?

✅ **Keeps main repo clean**: Only maintainers can push directly
✅ **Security**: Prevents unauthorized changes to the main codebase
✅ **Review process**: All changes go through PR review
✅ **Standard practice**: Used by 99% of open-source projects
✅ **Your safety net**: You can experiment in your fork without affecting the original

## Step-by-Step: Creating Your Pull Request

### Step 1: Fork the Repository on GitHub (First Time Only)

#### 1.1 Navigate to the Repository
Go to: https://github.com/databrickslabs/lakeflow-community-connectors

#### 1.2 Click "Fork" Button
- Located in the top-right corner of the page
- Click it to create your personal copy

#### 1.3 Select Destination
- Choose your GitHub account (Fatiine) as the destination
- Keep the repository name the same (recommended)
- Optionally add a description: "Gmail connector for Lakeflow - Hackathon 2026"
- Keep "Copy the master branch only" checked (unless you need other branches)
- Click **"Create fork"**

#### 1.4 Wait for Fork Creation
- GitHub will create your fork (takes 10-30 seconds)
- You'll be redirected to your fork: `https://github.com/Fatiine/lakeflow-community-connectors`

✅ **Checkpoint**: You now have your own copy of the repository!

### Step 2: Configure Git Remotes (First Time Only)

You should already have this done, but here's the reference:

```bash
# Check current remotes
git remote -v

# You should see:
# origin  git@github.com:databrickslabs/lakeflow-community-connectors.git (fetch)
# origin  git@github.com:databrickslabs/lakeflow-community-connectors.git (push)
# fork    git@github.com:Fatiine/lakeflow-community-connectors.git (fetch)
# fork    git@github.com:Fatiine/lakeflow-community-connectors.git (push)
```

**Best Practice Naming Convention:**
- `origin` = The original upstream repository (databrickslabs)
- `fork` = Your personal fork (Fatiine)

Alternative naming (some developers prefer):
- `upstream` = The original repository
- `origin` = Your fork

Either convention is fine - we're using `origin`/`fork` here.

✅ **Checkpoint**: Git knows about both repositories!

### Step 3: Push Your Branch to Your Fork

```bash
# Make sure you're on your feature branch
git branch --show-current
# Should show: feature/gmail-connector-hackathon-2026

# Push to your fork
git push -u fork feature/gmail-connector-hackathon-2026
```

**What this does:**
- Uploads your branch to your GitHub fork
- `-u` flag sets up tracking (so future `git push` commands know where to push)
- Creates the branch on your fork if it doesn't exist

**Expected output:**
```
Enumerating objects: XX, done.
Counting objects: 100% (XX/XX), done.
Delta compression using up to X threads
Compressing objects: 100% (XX/XX), done.
Writing objects: 100% (XX/XX), XX.XX KiB | XX.XX MiB/s, done.
Total XX (delta XX), reused XX (delta XX), pack-reused 0
remote: Resolving deltas: 100% (XX/XX), done.
To github.com:Fatiine/lakeflow-community-connectors.git
 * [new branch]      feature/gmail-connector-hackathon-2026 -> feature/gmail-connector-hackathon-2026
Branch 'feature/gmail-connector-hackathon-2026' set up to track remote branch 'feature/gmail-connector-hackathon-2026' from 'fork'.
```

✅ **Checkpoint**: Your code is now on GitHub in your fork!

### Step 4: Create Pull Request on GitHub

#### 4.1 Navigate to Your Fork
Go to: `https://github.com/Fatiine/lakeflow-community-connectors`

#### 4.2 GitHub Should Show a Banner
After pushing, GitHub typically shows a yellow/green banner:
```
feature/gmail-connector-hackathon-2026 had recent pushes X minutes ago
[Compare & pull request] button
```

Click the **"Compare & pull request"** button.

**Alternative method** (if no banner):
1. Click the "Pull requests" tab in your fork
2. Click "New pull request"
3. Click "compare across forks"
4. Set:
   - **base repository**: `databrickslabs/lakeflow-community-connectors`
   - **base branch**: `master` (or `main` - check what they use)
   - **head repository**: `Fatiine/lakeflow-community-connectors`
   - **compare branch**: `feature/gmail-connector-hackathon-2026`

#### 4.3 Fill Out the Pull Request Form

**Title** (keep it concise):
```
feat: Add Gmail community connector for email data ingestion
```

**Description** (copy from PULL_REQUEST_TEMPLATE.md):
- Open `sources/gmail/PULL_REQUEST_TEMPLATE.md`
- Copy the entire content
- Paste into the PR description field

**Additional PR settings:**

1. **Reviewers** (optional):
   - If you know who should review, add them
   - For hackathon, leave empty - organizers will assign

2. **Labels** (if available):
   - `hackathon-2026`
   - `community-connector`
   - `enhancement`

3. **Milestone** (if available):
   - Select "2026 Hackathon" if it exists

4. **Allow edits from maintainers** (checkbox):
   - ✅ **Check this box** - allows maintainers to make small fixes directly

#### 4.4 Review Your Changes

Before submitting, click the "Files changed" tab to review:
- ✅ All expected files are included
- ✅ No accidental files (like `__pycache__`, credentials)
- ✅ Code looks correct
- ✅ No merge conflicts

#### 4.5 Submit the Pull Request

Click the green **"Create pull request"** button.

✅ **Checkpoint**: Your PR is now live!

### Step 5: Share PR Link

After creation, GitHub will show your PR with a URL like:
```
https://github.com/databrickslabs/lakeflow-community-connectors/pull/XXX
```

**Share this link with:**
- Hackathon submission form
- Hackathon organizers
- Slack channel (#2026-community-connector-hackathon)
- Your team/manager

## Best Practices for Pull Requests

### ✅ DO

1. **Write Clear PR Description**
   - What problem does it solve?
   - How does it work?
   - How to test it?
   - Any breaking changes?

2. **Keep PR Focused**
   - One feature per PR
   - Related changes grouped together
   - Don't mix unrelated fixes

3. **Test Before Submitting**
   - All tests pass locally
   - Code runs without errors
   - Documentation is accurate

4. **Respond to Feedback Promptly**
   - Be open to suggestions
   - Make requested changes
   - Explain your reasoning if you disagree

5. **Keep PR Updated**
   - Rebase if requested
   - Resolve merge conflicts quickly
   - Keep branch in sync with master

6. **Use Good Commit Messages**
   - Clear, descriptive
   - Follow conventional commits format
   - Include context in commit body

### ❌ DON'T

1. **Don't Force Push After Review**
   - Makes it hard to see what changed
   - Exception: If explicitly asked to rebase

2. **Don't Submit Unfinished Work**
   - Mark as "Draft PR" if still working
   - Wait until ready for review

3. **Don't Include Credentials**
   - Double-check no sensitive data
   - Use `.gitignore` properly
   - Never commit API keys, tokens, passwords

4. **Don't Make PR Too Large**
   - Hard to review 1000+ line PRs
   - Split into multiple PRs if needed
   - Exception: Initial connector implementation

5. **Don't Ignore CI/CD Failures**
   - Fix failing tests
   - Address linting errors
   - Investigate build failures

6. **Don't Commit Generated Files** (usually)
   - Exception: `_generated_gmail_python_source.py` is required for this project

## Handling PR Feedback

### When Reviewers Request Changes

#### 1. Read Feedback Carefully
- Understand what's being asked
- Ask questions if unclear
- Don't take it personally (code review is about code quality)

#### 2. Make Changes Locally

```bash
# Make sure you're on your feature branch
git checkout feature/gmail-connector-hackathon-2026

# Make the requested changes
# ... edit files ...

# Stage changes
git add sources/gmail/gmail.py  # example

# Commit with clear message
git commit -m "fix: address PR feedback - improve error handling in OAuth flow

- Add more detailed error messages
- Handle edge case for expired refresh tokens
- Update documentation with troubleshooting tips"

# Push to your fork (updates the PR automatically)
git push fork feature/gmail-connector-hackathon-2026
```

#### 3. Respond to Comments
- Mark each feedback item as resolved when addressed
- Add a comment explaining what you changed
- Link to the commit that addresses the feedback

**Example response:**
```
Fixed in commit abc1234. I added try-catch blocks around the OAuth flow
and improved error messages as suggested. Let me know if this addresses
your concern!
```

### If Asked to Rebase

```bash
# Fetch latest changes from upstream
git fetch origin master

# Rebase your branch onto latest master
git rebase origin/master

# Resolve any conflicts if they appear
# ... fix conflicts ...
# git add <resolved-files>
# git rebase --continue

# Force push to update PR (only time force push is OK)
git push fork feature/gmail-connector-hackathon-2026 --force-with-lease
```

**Note**: `--force-with-lease` is safer than `--force` - it prevents accidentally overwriting others' work.

### If Asked to Squash Commits

Sometimes reviewers want you to combine multiple commits into one:

```bash
# Interactive rebase for last N commits
git rebase -i HEAD~6  # for your 6 commits

# In the editor, change "pick" to "squash" (or "s") for commits you want to combine
# Keep "pick" for the first commit
# Save and close

# Edit the combined commit message
# Save and close

# Force push
git push fork feature/gmail-connector-hackathon-2026 --force-with-lease
```

## Keeping Your Fork in Sync

### After Your PR is Merged

```bash
# Switch to master branch
git checkout master

# Fetch and merge changes from upstream
git pull origin master

# Push to your fork to keep it in sync
git push fork master

# Delete your feature branch (cleanup)
git branch -d feature/gmail-connector-hackathon-2026
git push fork --delete feature/gmail-connector-hackathon-2026
```

### Before Starting a New Feature

```bash
# Update your master branch
git checkout master
git pull origin master

# Create new feature branch from updated master
git checkout -b feature/my-new-feature

# Start working on new feature
```

## Common Issues and Solutions

### Issue 1: "Permission Denied" When Pushing

**Problem**: You're trying to push to origin (main repo) instead of your fork

**Solution**:
```bash
# Check which remote you're pushing to
git remote -v

# Push to fork, not origin
git push fork feature/gmail-connector-hackathon-2026
```

### Issue 2: Merge Conflicts

**Problem**: Your branch conflicts with changes in master

**Solution**:
```bash
# Fetch latest master
git fetch origin master

# Rebase onto master
git rebase origin/master

# Resolve conflicts in your editor
# After fixing each file:
git add <fixed-file>

# Continue rebase
git rebase --continue

# If you want to abort:
git rebase --abort

# Push updated branch
git push fork feature/gmail-connector-hackathon-2026 --force-with-lease
```

### Issue 3: Forgot to Create Fork

**Problem**: You have commits but no fork on GitHub

**Solution**:
1. Go to GitHub and fork the repository now
2. Add fork as remote (if not already done)
3. Push your branch to your fork

### Issue 4: Made Changes to Wrong Branch

**Problem**: You committed to `master` instead of feature branch

**Solution**:
```bash
# Create feature branch from current master
git checkout -b feature/gmail-connector-hackathon-2026

# Reset master to upstream (discards your commits on master)
git checkout master
git reset --hard origin/master

# Your commits are now on the feature branch only
git checkout feature/gmail-connector-hackathon-2026
git push fork feature/gmail-connector-hackathon-2026
```

### Issue 5: Accidentally Committed Credentials

**Problem**: You committed sensitive data (API keys, tokens, passwords)

**Solution**:
```bash
# Remove the file and commit
git rm sources/gmail/configs/dev_config.json
git commit -m "security: remove accidentally committed credentials"

# Rewrite history to remove from all commits
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch sources/gmail/configs/dev_config.json" \
  --prune-empty --tag-name-filter cat -- --all

# Force push (history was rewritten)
git push fork feature/gmail-connector-hackathon-2026 --force

# IMPORTANT: Rotate the compromised credentials immediately!
```

**Better**: Prevent this with `.gitignore` (which we already added)

## PR Etiquette and Communication

### Writing Good Comments

✅ **Good**:
```
Thanks for the review! I've addressed your feedback in commit abc1234:

1. Added error handling for expired tokens
2. Improved documentation for OAuth setup
3. Added test case for token refresh scenario

The one thing I kept as-is was the schema naming - I think "payload"
matches Gmail's API terminology and will be more intuitive for users
familiar with Gmail. Let me know if you'd prefer a different name!
```

❌ **Bad**:
```
done
```

### Being Professional

- **Be respectful**: Remember there are humans on the other side
- **Be patient**: Reviews take time, especially for large PRs
- **Be humble**: Be open to learning and improving
- **Say thank you**: Appreciate the time reviewers spend

### Following Up

If no response after a reasonable time:
- Wait 2-3 business days before following up
- Add a polite comment: "Hi! Just checking if you had a chance to review this. No rush, just wanted to make sure it's on your radar."
- For hackathon: Check Slack or hackathon channels

## Hackathon-Specific Tips

### For This Hackathon

1. **PR Title Format**:
   ```
   [Hackathon 2026] feat: Add Gmail community connector
   ```

2. **Tag Organizers** (if you know their GitHub handles):
   ```
   @hackathon-organizer I've submitted my Gmail connector for review!
   ```

3. **Link to Demo**:
   Include in PR description:
   ```
   **Demo Account**: DEMO_EMAIL_PLACEHOLDER (credentials in DEMO_ACCESS.md)
   **Test Command**: `pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v`
   ```

4. **Timeline Awareness**:
   - Submit PR well before deadline
   - Allow time for feedback and revisions
   - Be responsive during hackathon period

## Checklist Before Submitting PR

Use this checklist to ensure your PR is ready:

### Code Quality
- [ ] All tests passing locally
- [ ] Code follows project style guidelines
- [ ] No linting errors
- [ ] No debugging code left in (print statements, commented code)
- [ ] Error handling implemented
- [ ] Edge cases considered

### Documentation
- [ ] README updated with new connector
- [ ] Setup instructions clear and complete
- [ ] Code comments added for complex logic
- [ ] API documentation generated
- [ ] Examples provided

### Security
- [ ] No credentials committed
- [ ] `.gitignore` properly configured
- [ ] Dependencies reviewed for vulnerabilities
- [ ] OAuth scopes minimized (least privilege)

### Testing
- [ ] All existing tests still pass
- [ ] New tests added for new functionality
- [ ] Tests run against real API (not just mocks)
- [ ] Edge cases tested
- [ ] Error scenarios tested

### Git
- [ ] Branch name follows convention
- [ ] Commits have clear messages
- [ ] No merge commits (rebased if needed)
- [ ] No "WIP" or "temp" commits
- [ ] Co-author attribution added

### PR Description
- [ ] Title is clear and descriptive
- [ ] Description explains what and why
- [ ] Testing instructions provided
- [ ] Screenshots/examples included (if applicable)
- [ ] Breaking changes documented
- [ ] Related issues linked

### General
- [ ] PR is focused (one feature)
- [ ] No unrelated changes
- [ ] Generated files included (if required)
- [ ] Dependencies documented
- [ ] License compatibility checked

## Resources

- **GitHub Docs**: https://docs.github.com/en/pull-requests
- **Git Best Practices**: https://git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project
- **Conventional Commits**: https://www.conventionalcommits.org/
- **Code Review Guide**: https://google.github.io/eng-practices/review/

---

## Quick Reference Commands

```bash
# Fork workflow setup (one-time)
# 1. Fork on GitHub
# 2. Add fork as remote
git remote add fork git@github.com:Fatiine/lakeflow-community-connectors.git

# Working on feature
git checkout -b feature/my-feature
# ... make changes ...
git add .
git commit -m "feat: descriptive message"
git push fork feature/my-feature

# Updating PR after feedback
# ... make changes ...
git add .
git commit -m "fix: address review feedback"
git push fork feature/my-feature

# Keeping fork in sync
git checkout master
git pull origin master
git push fork master

# Clean up after merge
git branch -d feature/my-feature
git push fork --delete feature/my-feature
```

---

**Good luck with your pull request!** 🚀

This workflow is the industry standard and will serve you well for all future open-source contributions!
