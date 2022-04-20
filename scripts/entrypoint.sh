#!/usr/bin/env bash

# Resolve hostname to fix OID generation error by cx_Oracle
# Reference: https://stackoverflow.com/a/40400117
HOSTNAME=$(curl -s http://169.254.169.254/latest/meta-data/local-hostname | cut -d. -f1)
# Let's try something that is resolveable no matter what
HOSTNAME="phila.gov"
echo "$HOSTNAME localhost" > /tmp/HOSTALIASES
echo -e "\nGot hostname: $HOSTNAME from resolver."

echo "Force overwrite /bin/hostname with an echo.."
echo "echo $HOSTNAME" > /bin/hostname

SYS_HOSTNAME=$(hostname)
echo -e "\nWhat is our system hostname?: $SYS_HOSTNAME" 

echo -e "\nAttempting to ping $SYS_HOSTNAME.."
ping -c 1 $SYS_HOSTNAME

echo -e "\nAttempting to ping $HOSTNAME.."
ping -c 1 $HOSTNAME

echo -e "\nWhat's in resolv.conf?"
cat /etc/resolv.conf

echo -e "\nShowing contents of /etc/hosts.."
cat /etc/hosts

# This is needed to use both an entrypoint and a command with Docker
exec "${@}"
