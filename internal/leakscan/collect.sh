#!/bin/sh
# Collects what a sandbox holds into one tar for the leak scan, run as root
umask 077
W=/tmp/leak
S=$W/stage
rm -rf "$W"
mkdir -p "$S/proc" "$S/metadata"

for d in /proc/[0-9]*; do
	pid=${d#/proc/}
	mkdir -p "$S/proc/$pid"
	cat "$d/environ" > "$S/proc/$pid/environ" 2> /dev/null
	cat "$d/cmdline" > "$S/proc/$pid/cmdline" 2> /dev/null
done
ps -eo pid,user,args > "$S/ps.txt" 2>&1

if command -v systemctl > /dev/null 2>&1 && [ -d /run/systemd/system ]; then
	for u in $(systemctl list-units --all --plain --no-legend --no-pager 2> /dev/null |
		awk '{print $1}'); do
		echo "# $u"
		systemctl show -p Environment "$u" 2> /dev/null
	done > "$S/units.txt"
fi

# A metadata endpoint that answers at all fails the scan
probe() {
	if command -v curl > /dev/null 2>&1; then
		code=$(curl -g -s -m 2 -o /dev/null -w '%{http_code}' "$2" 2> /dev/null) &&
			[ "$code" != 000 ] && echo "$code" > "$S/metadata/$1"
	elif command -v bash > /dev/null 2>&1; then
		timeout 2 bash -c "exec 3<>/dev/tcp/$3/80" 2> /dev/null && echo open > "$S/metadata/$1"
	fi
}
probe imds-v4 http://169.254.169.254/ 169.254.169.254
probe imds-v6 'http://[fd00:ec2::254]/' fd00:ec2::254

cd / || exit 1
: > "$W/list"
: > "$S/large.txt"
for r in etc root home run tmp var/lib/cloud var/log; do
	[ -d "$r" ] || continue
	find "$r" -path tmp/leak -prune -o -type f -size -1025k -print0 >> "$W/list" 2> /dev/null
	find "$r" -path tmp/leak -prune -o -type f -size +1024k -print >> "$S/large.txt" 2> /dev/null
done

# The head cap keeps a huge sandbox from filling the disk, and the scan fails at the cap
tar --null --ignore-failed-read --warning=no-file-changed -cf - -C "$S" . -C / -T "$W/list" \
	2> /dev/null | head -c 67108864 > "$W/archive.tar"
rm -rf "$S" "$W/list"
chmod 755 "$W"
chmod 644 "$W/archive.tar"
wc -c < "$W/archive.tar"
