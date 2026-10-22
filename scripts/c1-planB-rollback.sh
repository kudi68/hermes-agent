#!/bin/bash
# DRAFT ONLY. Run only after explicit approval for the live C1 rollback.
set -euo pipefail

runtime_root=/Users/kudi68-15mba/Projects/c1-runtime/hermes-agent-c1-fe6ab90
night_root=/Users/kudi68-15mba/Projects/night/20261022
plist=/Users/kudi68-15mba/Library/LaunchAgents/local.hermes.c1-chief.plist
plist_a="$night_root/plist-A/local.hermes.c1-chief.plist"
base=b6e7d10e8b04fd10e7a1d72563a40c343a11181e
domain="gui/$(id -u)"
label=local.hermes.c1-chief

ls -ld "$runtime_root" "$plist" "$plist_a" "$plist_a.sha256"
shasum -a 256 -c "$plist_a.sha256"
plutil -lint "$plist_a"
git -C "$runtime_root" diff --quiet
git -C "$runtime_root" diff --cached --quiet
git -C "$runtime_root" cat-file -e "$base^{commit}"

# Detached checkout preserves feat/c1-wiring and refuses conflicting untracked files.
git -C "$runtime_root" checkout --detach "$base"
test "$(git -C "$runtime_root" rev-parse HEAD)" = "$base"
cp -p "$plist_a" "$plist"
cmp -s "$plist_a" "$plist"
plutil -lint "$plist"
launchctl bootout "$domain/$label"
launchctl bootstrap "$domain" "$plist"
launchctl print "$domain/$label"
