# Push google_sheets_docs connector to your fork

Your connector work is on the branch **`feature/google_sheets_docs`**. To save it to your GitHub fork (no PR yet):

## Step 1: Create the fork on GitHub

1. Open: **https://github.com/databrickslabs/lakeflow-community-connectors**
2. Click the **Fork** button (top right).
3. Create the fork under your account (e.g. `dgalluzzo26`).
4. Wait until the fork is created (you’ll be on `https://github.com/dgalluzzo26/lakeflow-community-connectors`).

## Step 2: Push your branch to the fork

From the repo root, run:

```bash
git push -u myfork feature/google_sheets_docs
```

Your code will be on:

**https://github.com/dgalluzzo26/lakeflow-community-connectors/tree/feature/google_sheets_docs**

---

**Remotes:**  
- `origin` = databrickslabs/lakeflow-community-connectors (upstream)  
- `myfork` = dgalluzzo26/lakeflow-community-connectors (your fork)

If your GitHub username is not `dgalluzzo26`, fix the remote first:

```bash
git remote set-url myfork https://github.com/YOUR_USERNAME/lakeflow-community-connectors.git
```

Then run the push command above.
