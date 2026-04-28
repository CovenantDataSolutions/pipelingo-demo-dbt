#!/usr/bin/env bash
# Generate an unencrypted RSA key-pair for the Snowflake CI service user
# (PIPELINGO_DBT_SVC) and print the next steps. Output files are written
# to ./setup/keys/ which is .gitignored.
#
# Usage: bash setup/generate_keypair.sh
set -euo pipefail

KEYDIR="$(dirname "$0")/keys"
mkdir -p "$KEYDIR"

if [ -f "$KEYDIR/rsa_key.p8" ]; then
  echo "❌ $KEYDIR/rsa_key.p8 already exists. Delete it first if you want to rotate." >&2
  exit 1
fi

# 1) Private key (PKCS8, unencrypted — simplest for CI; encrypt with -out + -aes-256-cbc if you prefer)
openssl genrsa 2048 \
  | openssl pkcs8 -topk8 -inform PEM -out "$KEYDIR/rsa_key.p8" -nocrypt

# 2) Public key (the format Snowflake's ALTER USER expects)
openssl rsa -in "$KEYDIR/rsa_key.p8" -pubout -out "$KEYDIR/rsa_key.pub"

# 3) The exact strings to paste downstream
echo
echo "================================================================"
echo "  Step 1 — Paste this into Snowsight (where the SQL says <PASTE_PUBLIC_KEY_HERE>):"
echo "================================================================"
sed -e '/-----BEGIN/d' -e '/-----END/d' "$KEYDIR/rsa_key.pub" | tr -d '\n'
echo
echo
echo "================================================================"
echo "  Step 2 — Add the FULL contents of $KEYDIR/rsa_key.p8 (including"
echo "  the BEGIN/END lines, with newlines) as a GitHub secret named:"
echo "      SNOWFLAKE_PRIVATE_KEY"
echo "================================================================"
echo
echo "  cat \"$KEYDIR/rsa_key.p8\" | pbcopy   # macOS — copies to clipboard"
echo
echo "Files generated:"
echo "  - $KEYDIR/rsa_key.p8   (private — never commit)"
echo "  - $KEYDIR/rsa_key.pub  (public — safe but pointless to commit)"
