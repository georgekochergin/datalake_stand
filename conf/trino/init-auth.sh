#!/bin/sh
set -eu
umask 077
htpasswd -niBC 10 "$STAND_USER" <<EOF > /auth/password.db
$STAND_PASSWORD
EOF
chmod 644 /auth/password.db
