#!/bin/bash
# install_monitoring.sh
# Installs Prometheus, Pushgateway, Node Exporter, Grafana on bare Azure VM.
# Prometheus TSDB → /mnt/prometheus_data  (24G free, keeps root partition safe)
# Run: chmod +x install_monitoring.sh && ./install_monitoring.sh

set -e

PROM_VERSION="2.51.0"
PUSH_VERSION="1.8.0"
NODE_VERSION="1.7.0"
PROJECT_DIR="$HOME/azure_analysis_algorithm"

echo "════════════════════════════════════════════════════"
echo " Sales Analysis Pipeline — Monitoring Setup"
echo " Prometheus TSDB → /mnt/prometheus_data"
echo "════════════════════════════════════════════════════"

# ── 1. Prometheus ─────────────────────────────────────────────────────────────
echo ""
echo "▶ [1/5] Prometheus ${PROM_VERSION}..."
sudo useradd --no-create-home --shell /bin/false prometheus 2>/dev/null || true
cd /tmp
wget -q "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/prometheus-${PROM_VERSION}.linux-amd64.tar.gz"
tar xf "prometheus-${PROM_VERSION}.linux-amd64.tar.gz"
sudo cp "prometheus-${PROM_VERSION}.linux-amd64/prometheus" /usr/local/bin/
sudo cp "prometheus-${PROM_VERSION}.linux-amd64/promtool"   /usr/local/bin/
sudo mkdir -p /etc/prometheus
sudo cp -r "prometheus-${PROM_VERSION}.linux-amd64/consoles"          /etc/prometheus/
sudo cp -r "prometheus-${PROM_VERSION}.linux-amd64/console_libraries" /etc/prometheus/
sudo mkdir -p /mnt/prometheus_data
sudo chown prometheus:prometheus /mnt/prometheus_data
sudo chown -R prometheus:prometheus /etc/prometheus
sudo chown prometheus:prometheus /usr/local/bin/prometheus /usr/local/bin/promtool

sudo tee /etc/prometheus/prometheus.yml > /dev/null << 'EOF'
global:
  scrape_interval:     15s
  evaluation_interval: 15s
  external_labels:
    project: "sales_analysis"
    host:    "azure-vm"

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: "pushgateway"
    honor_labels: true
    static_configs:
      - targets: ["localhost:9091"]

  - job_name: "node"
    static_configs:
      - targets: ["localhost:9100"]
EOF

sudo tee /etc/systemd/system/prometheus.service > /dev/null << 'EOF'
[Unit]
Description=Prometheus
Wants=network-online.target
After=network-online.target

[Service]
User=prometheus
Group=prometheus
Type=simple
ExecStart=/usr/local/bin/prometheus \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/mnt/prometheus_data/ \
    --storage.tsdb.retention.time=15d \
    --web.console.templates=/etc/prometheus/consoles \
    --web.console.libraries=/etc/prometheus/console_libraries \
    --web.listen-address=0.0.0.0:9090 \
    --web.enable-lifecycle
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now prometheus
echo "   ✅ Prometheus :9090  (data → /mnt/prometheus_data)"

# ── 2. Pushgateway ────────────────────────────────────────────────────────────
echo ""
echo "▶ [2/5] Pushgateway ${PUSH_VERSION}..."
sudo useradd --no-create-home --shell /bin/false pushgateway 2>/dev/null || true
cd /tmp
wget -q "https://github.com/prometheus/pushgateway/releases/download/v${PUSH_VERSION}/pushgateway-${PUSH_VERSION}.linux-amd64.tar.gz"
tar xf "pushgateway-${PUSH_VERSION}.linux-amd64.tar.gz"
sudo cp "pushgateway-${PUSH_VERSION}.linux-amd64/pushgateway" /usr/local/bin/
sudo chown pushgateway:pushgateway /usr/local/bin/pushgateway

sudo tee /etc/systemd/system/pushgateway.service > /dev/null << 'EOF'
[Unit]
Description=Prometheus Pushgateway
Wants=network-online.target
After=network-online.target

[Service]
User=pushgateway
Group=pushgateway
Type=simple
ExecStart=/usr/local/bin/pushgateway --web.listen-address=0.0.0.0:9091
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now pushgateway
echo "   ✅ Pushgateway :9091"

# ── 3. Node Exporter ──────────────────────────────────────────────────────────
echo ""
echo "▶ [3/5] Node Exporter ${NODE_VERSION}..."
sudo useradd --no-create-home --shell /bin/false node_exporter 2>/dev/null || true
cd /tmp
wget -q "https://github.com/prometheus/node_exporter/releases/download/v${NODE_VERSION}/node_exporter-${NODE_VERSION}.linux-amd64.tar.gz"
tar xf "node_exporter-${NODE_VERSION}.linux-amd64.tar.gz"
sudo cp "node_exporter-${NODE_VERSION}.linux-amd64/node_exporter" /usr/local/bin/
sudo chown node_exporter:node_exporter /usr/local/bin/node_exporter

sudo tee /etc/systemd/system/node_exporter.service > /dev/null << 'EOF'
[Unit]
Description=Node Exporter
Wants=network-online.target
After=network-online.target

[Service]
User=node_exporter
Group=node_exporter
Type=simple
ExecStart=/usr/local/bin/node_exporter
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now node_exporter
echo "   ✅ Node Exporter :9100"

# ── 4. Grafana ────────────────────────────────────────────────────────────────
echo ""
echo "▶ [4/5] Grafana..."
sudo apt-get install -y apt-transport-https software-properties-common wget 2>/dev/null
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update -q
sudo apt-get install -y grafana

sudo mkdir -p /etc/grafana/provisioning/datasources
sudo tee /etc/grafana/provisioning/datasources/prometheus.yml > /dev/null << 'EOF'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://localhost:9090
    isDefault: true
    editable: false
EOF

sudo mkdir -p /etc/grafana/provisioning/dashboards
sudo tee /etc/grafana/provisioning/dashboards/sales.yml > /dev/null << 'EOF'
apiVersion: 1
providers:
  - name: "Sales Analysis"
    orgId: 1
    folder: "Sales Pipeline"
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
EOF

sudo mkdir -p /var/lib/grafana/dashboards

# Copy dashboard JSON if it exists next to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASH_SRC="$SCRIPT_DIR/monitoring/grafana/dashboards/sales_pipeline.json"
if [ -f "$DASH_SRC" ]; then
    sudo cp "$DASH_SRC" /var/lib/grafana/dashboards/sales_pipeline.json
    echo "   ✅ Dashboard JSON installed"
else
    echo "   ⚠️  Dashboard JSON not found at $DASH_SRC"
    echo "      Upload sales_pipeline.json to /var/lib/grafana/dashboards/ manually"
fi

sudo chown -R grafana:grafana /var/lib/grafana/dashboards
sudo systemctl enable --now grafana-server
echo "   ✅ Grafana :3000  (admin / admin)"

# ── 5. Python metrics module ──────────────────────────────────────────────────
echo ""
echo "▶ [5/5] Python metrics module..."

pip install prometheus-client --break-system-packages 2>/dev/null || \
    pip3 install prometheus-client 2>/dev/null || \
    echo "   ⚠️  Run manually: pip install prometheus-client"

mkdir -p "$PROJECT_DIR/monitoring"
touch "$PROJECT_DIR/monitoring/__init__.py"

# Copy metrics.py from next to this script
METRICS_SRC="$SCRIPT_DIR/monitoring/metrics.py"
if [ -f "$METRICS_SRC" ]; then
    cp "$METRICS_SRC" "$PROJECT_DIR/monitoring/metrics.py"
    echo "   ✅ monitoring/metrics.py → $PROJECT_DIR/monitoring/"
else
    echo "   ⚠️  metrics.py not found — copy it manually to $PROJECT_DIR/monitoring/"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════"
echo " ✅  Done. Service status:"
echo "════════════════════════════════════════════════════"
sudo systemctl is-active prometheus     && echo "   Prometheus    ✅  :9090" || echo "   Prometheus    ❌  sudo journalctl -u prometheus -n 20"
sudo systemctl is-active pushgateway    && echo "   Pushgateway   ✅  :9091" || echo "   Pushgateway   ❌  sudo journalctl -u pushgateway -n 20"
sudo systemctl is-active node_exporter  && echo "   Node Exporter ✅  :9100" || echo "   Node Exporter ❌  sudo journalctl -u node_exporter -n 20"
sudo systemctl is-active grafana-server && echo "   Grafana       ✅  :3000  (admin / admin)" || echo "   Grafana       ❌  sudo journalctl -u grafana-server -n 20"
echo ""
echo "  Disk — Prometheus TSDB on /mnt:"
df -h /mnt | tail -1
echo ""
echo "  ⚠️  Azure portal → VM → Networking → Add inbound rules:"
echo "     Port 3000 (Grafana)    — restrict to your IP"
echo "     Port 9090 (Prometheus) — optional, restrict to your IP"
echo ""
echo "  Change default password:"
echo "     sudo grafana-cli admin reset-admin-password <newpassword>"
echo "     sudo systemctl restart grafana-server"
echo ""
echo "  Next: add metrics calls — see monitoring/INTEGRATION.py"
echo "════════════════════════════════════════════════════"