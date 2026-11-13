#!/usr/bin/env bash
# Is the simulator running, what is it producing, and do messages decode?
source /opt/rs/sim_host/env.sh
echo "service: $(systemctl is-active rs-sim || true)"
echo "--- last stats ---"
sudo journalctl -u rs-sim --no-pager -n 3 -o cat
echo "--- newest messages (decoded via registry) ---"
cd /opt/rs/sim_host/simulator
/opt/rs/venv/bin/python peek.py gps_pings 3
/opt/rs/venv/bin/python peek.py ride_events 3
