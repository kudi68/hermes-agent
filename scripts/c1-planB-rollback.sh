#!/usr/bin/env bash
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

# A failed deployment may already have unloaded the job. Other failures must stop rollback.
if bootout_error=$(launchctl bootout "$domain/$label" 2>&1); then
  :
else
  bootout_status=$?
  case "$bootout_error" in
    *"No such process"*|*"Could not find service"*)
      printf 'C1 job is already unloaded; continuing rollback.\n'
      ;;
    *)
      printf '%s\n' "$bootout_error" >&2
      exit "$bootout_status"
      ;;
  esac
fi

# launchctl can return from bootout before the job disappears. Do not touch the
# checkout or plist until the exact service lookup reports it is gone.
job_unloaded=false
for attempt in {1..35}; do
  if job_state=$(launchctl print "$domain/$label" 2>&1); then
    if [ "$attempt" -lt 35 ]; then sleep 1; fi
    continue
  else
    print_status=$?
    case "$job_state" in
      *"Could not find service \"$label\" in domain"*)
        job_unloaded=true
        printf 'C1 job is unloaded; continuing rollback.\n'
        break
        ;;
      *)
        printf '%s\n' "$job_state" >&2
        exit "$print_status"
        ;;
    esac
  fi
done
if [ "$job_unloaded" != true ]; then
  printf 'C1 job remained loaded for 35 checks; refusing checkout and bootstrap.\n' >&2
  exit 1
fi

# Detached checkout preserves feat/c1-wiring and refuses conflicting untracked files.
git -C "$runtime_root" checkout --detach "$base"
test "$(git -C "$runtime_root" rev-parse HEAD)" = "$base"
cp -p "$plist_a" "$plist"
cmp -s "$plist_a" "$plist"
plutil -lint "$plist"
launchctl bootstrap "$domain" "$plist"

# bootstrap may return before launchd has started the process.
for attempt in {1..35}; do
  job_state=$(launchctl print "$domain/$label")
  while IFS= read -r line; do
    if [[ "$line" =~ ^[[:space:]]*pid[[:space:]]*=[[:space:]]*([0-9]+)[[:space:]]*$ ]]; then
      printf '%s\n' "$job_state"
      expected_pid=${BASH_REMATCH[1]}
      process_list=$(pgrep -l .)
      process_found=false
      while IFS= read -r process_line; do
        if [[ "$process_line" =~ ^([0-9]+)[[:space:]] ]] \
          && [ "${BASH_REMATCH[1]}" = "$expected_pid" ]; then
          printf '%s\n' "$process_line"
          process_found=true
          break
        fi
      done <<< "$process_list"
      if [ "$process_found" != true ]; then
        printf 'launchd reported PID %s, but pgrep -l did not list it.\n' "$expected_pid" >&2
        exit 1
      fi
      exit 0
    fi
  done <<< "$job_state"
  if [ "$attempt" -lt 35 ]; then sleep 1; fi
done
printf 'C1 job did not publish a PID within 35 checks.\n' >&2
exit 1
