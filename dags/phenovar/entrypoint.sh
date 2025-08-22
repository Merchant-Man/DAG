#!/usr/bin/env bash
set -euo pipefail

# Initialize from env (safe with -u)
AES_KEY="${AES_ENCRYPTION_SECRET_KEY:-}"
AES_IV="${AES_ENCRYPTION_IV:-}"
RDS_SECRET="${RDS_SECRET:-}"
PHENOVAR_USERNAME="${PHENOVAR_USERNAME:-}"
PHENOVAR_PASSWORD="${PHENOVAR_PASSWORD:-}"

print_usage() {
  cat <<EOF
Usage: docker run ... IMAGE
  --key <AES_KEY>
  --iv <AES_IV>
  --rds_secret <RDS_SECRET>
  --phenovar_username <USERNAME>
  --phenovar_password <PASSWORD>
  -h, --help
You may also set these via environment variables of the same names.
EOF
}

# Parse flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --key)                [[ $# -ge 2 ]] || { echo "Missing value for --key"; print_usage; exit 64; }
                          AES_KEY="$2"; shift 2;;
    --iv)                 [[ $# -ge 2 ]] || { echo "Missing value for --iv"; print_usage; exit 64; }
                          AES_IV="$2"; shift 2;;
    --rds_secret)         [[ $# -ge 2 ]] || { echo "Missing value for --rds_secret"; print_usage; exit 64; }
                          RDS_SECRET="$2"; shift 2;;
    --phenovar_username)  [[ $# -ge 2 ]] || { echo "Missing value for --phenovar_username"; print_usage; exit 64; }
                          PHENOVAR_USERNAME="$2"; shift 2;;
    --phenovar_password)  [[ $# -ge 2 ]] || { echo "Missing value for --phenovar_password"; print_usage; exit 64; }
                          PHENOVAR_PASSWORD="$2"; shift 2;;
    -h|--help)            print_usage; exit 0;;
    --)                   shift; break;;
    *)                    echo "Unknown argument: $1"; print_usage; exit 64;;
  esac
done

# Validate (use ':-' so unset/empty both count as missing, without tripping -u)
if [[ -z "${AES_KEY:-}" || -z "${AES_IV:-}" || -z "${RDS_SECRET:-}" || -z "${PHENOVAR_USERNAME:-}" || -z "${PHENOVAR_PASSWORD:-}" ]]; then
  echo "Error: AES key, IV, RDS secret, Phenovar username, and password are required." >&2
  print_usage
  exit 64
fi

# Export for the Python app
export AES_ENCRYPTION_SECRET_KEY="$AES_KEY"
export AES_ENCRYPTION_IV="$AES_IV"
export RDS_SECRET="$RDS_SECRET"
export PHENOVAR_USERNAME="$PHENOVAR_USERNAME"
export PHENOVAR_PASSWORD="$PHENOVAR_PASSWORD"

exec python /app/phenovar_iron.py
