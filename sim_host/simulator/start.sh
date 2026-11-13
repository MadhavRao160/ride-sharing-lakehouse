#!/usr/bin/env bash
# Start (or restart with new settings) the simulator as a systemd service.
# Usage: start.sh [drivers=5000] [requests_per_sec=4]
# Not enabled at boot on purpose: a resumed EC2 must not start producing by itself.
source /opt/rs/sim_host/env.sh
DRIVERS="${1:-5000}"
REQ_RATE="${2:-4}"

sudo tee /etc/rs-sim.env >/dev/null <<EOF
BS=$BS
AWS_REGION=$AWS_REGION
DRIVERS=$DRIVERS
REQ_RATE=$REQ_RATE
DUP_RATE=0.01
LATE_RATE=0.005
EOF

sudo tee /etc/systemd/system/rs-sim.service >/dev/null <<'EOF'
[Unit]
Description=Ride-sharing simulator (gps_pings + ride_events)
After=network-online.target confluent-schema-registry.service

[Service]
User=ec2-user
EnvironmentFile=/etc/rs-sim.env
WorkingDirectory=/opt/rs/sim_host/simulator
ExecStart=/opt/rs/venv/bin/python -u /opt/rs/sim_host/simulator/sim.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl restart rs-sim
echo "started: drivers=$DRIVERS requests/s=$REQ_RATE; first stats in ~25s"
sleep 25
sudo journalctl -u rs-sim --no-pager -n 6 -o cat
systemctl is-active rs-sim
