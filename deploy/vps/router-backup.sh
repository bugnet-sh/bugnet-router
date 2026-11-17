#!/usr/bin/env bash
# Backs up the router's database and /etc/bugnet/keys into /var/backups/bugnet-router (founder
# pick 2a, 2026-11-16) and keeps the newest 14. Run as root: every night by
# bugnet-router-backup.timer, and by deploy.sh before it migrates.
#
#   sudo router-backup.sh [recipients file]
#
# It dumps the bugnet database with pg_dump --format=custom as postgres, checks the dump with
# pg_restore --list, streams it (database.dump) and the keys folder (keys/) as one tar through age
# to the public keys in the recipients file (default /etc/bugnet/router-backup-recipients.txt),
# and renames the result to router-<UTC stamp>.tar.age, mode 0600, in a root-only folder. The
# matching private key stays off the server, so nothing on the server can read a backup. The env
# files are never in it. A failed run exits non-zero and leaves no partial file. It prints paths
# and sizes, never a secret.
#
# BUGNET_ROUTER_BACKUP_DIR, _KEYS, _DB and _PG_USER point it elsewhere for the tests; an empty
# _PG_USER runs pg_dump as the calling user.
set -euo pipefail
umask 077

recipients=${1:-/etc/bugnet/router-backup-recipients.txt}
backup_dir=${BUGNET_ROUTER_BACKUP_DIR:-/var/backups/bugnet-router}
keys=${BUGNET_ROUTER_BACKUP_KEYS:-/etc/bugnet/keys}
db=${BUGNET_ROUTER_BACKUP_DB:-bugnet}
pg_user=${BUGNET_ROUTER_BACKUP_PG_USER-postgres}
keep=14

die() {
	printf 'router-backup.sh: %s\n' "$*" >&2
	exit 1
}

say() {
	printf 'router-backup.sh: %s\n' "$*"
}

command -v age >/dev/null || die "age is not installed; deploy.sh installs it"
grep -q '^age1[0-9a-z]*$' "$recipients" 2>/dev/null ||
	die "$recipients holds no age public key (one age1... line)"
[[ -d $keys ]] || die "$keys is missing"
recipients=$(cd "$(dirname "$recipients")" && pwd)/$(basename "$recipients")
keys=$(cd "$keys" && pwd)

# pg_dump as postgres cannot enter root's working directory and would warn about it
cd /
mkdir -p "$backup_dir"
chmod 0700 "$backup_dir"

stamp=$(date -u +%Y%m%dT%H%M%SZ)
final=$backup_dir/router-$stamp.tar.age
work=$(mktemp -d "$backup_dir/.router-$stamp.XXXXXX") ||
	die "cannot create a work folder in $backup_dir"
# shellcheck disable=SC2064
trap "rm -rf -- $(printf '%q' "$work")" EXIT
trap 'exit 1' INT TERM HUP

as_pg=()
[[ -z $pg_user ]] || as_pg=(runuser -u "$pg_user" --)
# postgres cannot write into the root-only folder, so the dump comes back through this shell
${as_pg[@]+"${as_pg[@]}"} pg_dump --format=custom "$db" >"$work/database.dump" ||
	die "pg_dump of $db failed; no backup was written"
pg_restore --list "$work/database.dump" >/dev/null ||
	die "pg_restore --list cannot read the dump of $db; no backup was written"
tar -c -f - -C "$work" database.dump -C "$(dirname "$keys")" "$(basename "$keys")" |
	age --encrypt --recipients-file "$recipients" --output "$work/out.age" ||
	die "tar or age failed; no backup was written"
if ! chmod 0600 "$work/out.age" || ! mv -f "$work/out.age" "$final"; then
	die "cannot rename the backup to $final"
fi
say "wrote $final ($(wc -c <"$final" | tr -d ' ') bytes)"

# The UTC stamp sorts by time. Only regular files named as this script names them count or go
re='^router-[0-9]{8}T[0-9]{6}Z[.]tar[.]age$'
for path in "$backup_dir"/router-*.tar.age; do
	if [[ ${path##*/} =~ $re && -f $path ]]; then echo "${path##*/}"; fi
done | sort -r | tail -n "+$((keep + 1))" | while IFS= read -r name; do
	rm -f -- "$backup_dir/$name"
	say "removed $name"
done

# A run killed outright (SIGKILL, power loss) skips its trap, and no run lasts a day
find "$backup_dir" -maxdepth 1 -type d -name '.router-????????T??????Z.??????' -mmin +1440 |
	while IFS= read -r path; do
		[[ $path == "$work" ]] && continue
		rm -rf -- "$path"
		say "removed ${path##*/}, left by a stopped run"
	done
