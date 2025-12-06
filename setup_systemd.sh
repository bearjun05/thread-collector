#!/usr/bin/env bash
# Systemd service installer for Threads Collector API
set -euo pipefail

SERVICE_NAME="thread-collector"
USER_NAME="${USER:-uj}"
WORKDIR="/home/${USER_NAME}/opt/thread-collector"
PORT="${PORT:-8001}"
PY_CMD="/usr/bin/env uv run uvicorn app:app --host 0.0.0.0 --port ${PORT}"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

if [[ $EUID -ne 0 ]]; then
  echo "이 스크립트는 systemd 유닛을 쓰므로 sudo로 실행하세요." >&2
  exit 1
fi

cat <<EOF > "${UNIT_FILE}"
[Unit]
Description=Threads Collector API
After=network.target

[Service]
Type=simple
User=${USER_NAME}
WorkingDirectory=${WORKDIR}
Environment="PORT=${PORT}"
ExecStart=${PY_CMD}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo "systemd 유닛 작성: ${UNIT_FILE}"

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"

echo "서비스 상태:"
systemctl status "${SERVICE_NAME}" --no-pager || true

echo
echo "로그 확인: sudo journalctl -u ${SERVICE_NAME} -f"

# 상태 확인
#  sudo systemctl status thread-collector --no-pager
# 로그 보기
#  sudo journalctl -u
# 재시작/중지
#  sudo systemctl restart thread-collector
#  sudo systemctl stop thread-collector