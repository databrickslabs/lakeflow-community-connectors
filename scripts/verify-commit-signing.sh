#!/usr/bin/env bash
# Run this in your own terminal (not inside Cursor) to fix "No signature" for SSH commit signing.
set -e
echo "=== 1. Git signing config ==="
git config --get commit.gpgsign || true
git config --get gpg.format || true
git config --get user.signingkey || true

echo ""
echo "=== 2. SSH agent – key must be loaded for signing (passphrase-protected keys) ==="
ssh-add -l 2>/dev/null || echo "Could not list agent (run in Terminal.app or iTerm)"

echo ""
echo "=== 3. Add your key to the agent (enter passphrase when prompted) ==="
echo "Run: ssh-add --apple-use-keychain ~/.ssh/id_ed25519"
echo "Then run: ssh-add -l  (you should see id_ed25519)"

echo ""
echo "=== 4. Create a signed test commit ==="
echo "Run: git commit --allow-empty -m 'chore: test signing'"
echo "Then: git log -1 --show-signature"
echo "You want to see 'Good signature' and your key. If you see 'No signature', the agent didn't have the key when Git tried to sign."

echo ""
echo "=== 5. Optional: force signing from Cursor ==="
echo "If you commit from Cursor, ensure the key is in the agent and Keychain so background processes can use it:"
echo "  ssh-add --apple-use-keychain ~/.ssh/id_ed25519"
echo "Add to ~/.ssh/config:"
echo "  Host *"
echo "    AddKeysToAgent yes"
echo "    UseKeychain yes"
echo "    IdentityFile ~/.ssh/id_ed25519"
