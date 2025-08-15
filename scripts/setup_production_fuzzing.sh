#!/bin/bash
#
# KausalDB Production Fuzzing Setup
#
# Sets up continuous fuzzing monitoring for production readiness validation.
# This script configures monitoring, alerting, and automated reporting for
# long-running fuzzing campaigns.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAUSALDB_ROOT="$(dirname "$SCRIPT_DIR")"
MONITORING_DIR="$KAUSALDB_ROOT/.fuzzing_monitoring"
NTFY_CONFIG="$MONITORING_DIR/ntfy_config"
FUZZING_LOG="$MONITORING_DIR/production_fuzzing.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

show_help() {
    cat << 'EOF'
KausalDB Production Fuzzing Setup

This script sets up continuous fuzzing monitoring infrastructure for production 
readiness validation. It configures automated crash reporting, performance 
monitoring, and optional notification systems.

USAGE:
    ./scripts/setup_production_fuzzing.sh [OPTIONS] [COMMAND]

COMMANDS:
    setup                 Set up production fuzzing infrastructure
    start TARGET          Start continuous fuzzing for specified target
    stop                  Stop all running fuzzing processes
    status                Show status of running fuzzing processes
    monitor               Show real-time monitoring dashboard
    clean                 Clean up monitoring infrastructure

OPTIONS:
    --notifications       Enable push notifications (requires ntfy.sh setup)
    --target TARGET       Fuzzing target (storage, query, parser, serialization, all)
    --help               Show this help message

EXAMPLES:
    # Initial setup
    ./scripts/setup_production_fuzzing.sh setup

    # Start continuous storage fuzzing with notifications
    ./scripts/setup_production_fuzzing.sh --notifications start storage

    # Monitor running fuzzing processes
    ./scripts/setup_production_fuzzing.sh monitor

    # Stop all fuzzing and show final report
    ./scripts/setup_production_fuzzing.sh stop

PRODUCTION DEPLOYMENT:
    # Run in background with full monitoring
    nohup ./scripts/setup_production_fuzzing.sh --notifications start all > fuzzing.log 2>&1 &

    # Set up as systemd service (Linux)
    sudo cp fuzzing.service /etc/systemd/system/
    sudo systemctl enable kausaldb-fuzzing
    sudo systemctl start kausaldb-fuzzing

For questions or support, see docs/FUZZING_COVERAGE_REPORT.md
EOF
}

setup_monitoring_infrastructure() {
    log_step "Setting up production fuzzing monitoring infrastructure"

    # Create monitoring directory structure
    mkdir -p "$MONITORING_DIR"
    mkdir -p "$MONITORING_DIR/reports"
    mkdir -p "$MONITORING_DIR/metrics"
    mkdir -p "$MONITORING_DIR/dashboards"

    # Create monitoring configuration
    cat > "$MONITORING_DIR/config.sh" << 'EOF'
#!/bin/bash
# KausalDB Production Fuzzing Configuration

# Notification settings
NTFY_TOPIC="kausaldb_fuzzing"
ENABLE_NOTIFICATIONS=false
NOTIFICATION_INTERVAL_MIN=60

# Performance monitoring
PERFORMANCE_CHECK_INTERVAL_MIN=30
PERFORMANCE_THRESHOLD_DEVIATION_PERCENT=20

# Crash reporting
MAX_CRASH_REPORTS=100
CRASH_CLEANUP_DAYS=7

# Resource monitoring  
MEMORY_THRESHOLD_MB=4096
DISK_THRESHOLD_GB=10

# Auto-restart settings
AUTO_RESTART_ON_CRASH=true
MAX_RESTART_ATTEMPTS=5
RESTART_BACKOFF_MIN=5
EOF

    # Create systemd service file template
    cat > "$MONITORING_DIR/kausaldb-fuzzing.service" << EOF
[Unit]
Description=KausalDB Continuous Fuzzing Service
After=network.target
Wants=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$KAUSALDB_ROOT
ExecStart=$SCRIPT_DIR/fuzz.sh production all
Restart=always
RestartSec=30
StandardOutput=append:$FUZZING_LOG
StandardError=append:$FUZZING_LOG

# Resource limits
MemoryMax=8G
CPUQuota=75%

[Install]
WantedBy=multi-user.target
EOF

    # Create monitoring dashboard script
    cat > "$MONITORING_DIR/dashboard.sh" << 'EOF'
#!/bin/bash
# Real-time fuzzing monitoring dashboard

MONITORING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$MONITORING_DIR/config.sh"

show_dashboard() {
    clear
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                KausalDB Fuzzing Monitor                      ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo
    
    # Show running processes
    echo "🔄 Active Fuzzing Processes:"
    ps aux | grep -E "(fuzz|storage|query)" | grep -v grep || echo "   No active fuzzing processes"
    echo
    
    # Show recent crashes
    echo "💥 Recent Crashes (last 24h):"
    find ../fuzz_reports -name "crash_*.txt" -mtime -1 2>/dev/null | head -5 | while read -r crash; do
        echo "   - $(basename "$crash")"
    done || echo "   No recent crashes"
    echo
    
    # Show performance metrics
    echo "📊 Performance Metrics:"
    if [[ -f "metrics/latest.txt" ]]; then
        cat "metrics/latest.txt"
    else
        echo "   No performance data available"
    fi
    echo
    
    # Show system resources
    echo "💻 System Resources:"
    echo "   Memory: $(ps -A -o rss= | awk '{sum+=$1} END {printf "%.1f MB", sum/1024}')"
    echo "   Disk: $(df -h . | tail -1 | awk '{print "Used: " $3 "/" $2 " (" $5 ")"}')"
    echo
    
    echo "Press Ctrl+C to exit monitor"
}

# Main monitoring loop
while true; do
    show_dashboard
    sleep 5
done
EOF
    chmod +x "$MONITORING_DIR/dashboard.sh"

    # Create performance monitoring script
    cat > "$MONITORING_DIR/performance_monitor.sh" << 'EOF'
#!/bin/bash
# Automated performance monitoring and alerting

MONITORING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$MONITORING_DIR/config.sh"

monitor_performance() {
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    local metrics_file="$MONITORING_DIR/metrics/$(date +%Y%m%d).txt"
    
    # Collect performance metrics
    local fuzzing_rate=$(ps aux | grep -E "fuzz.*storage" | grep -v grep | wc -l)
    local memory_usage=$(ps -A -o rss= | awk '{sum+=$1} END {printf "%.0f", sum/1024}')
    local crash_count=$(find ../fuzz_reports -name "crash_*.txt" -mtime -1 2>/dev/null | wc -l)
    
    # Log metrics
    echo "[$timestamp] Rate:${fuzzing_rate} Memory:${memory_usage}MB Crashes:${crash_count}" >> "$metrics_file"
    
    # Update latest metrics
    cat > "$MONITORING_DIR/metrics/latest.txt" << EOL
   Fuzzing Rate: $fuzzing_rate processes/min
   Memory Usage: ${memory_usage} MB (threshold: ${MEMORY_THRESHOLD_MB} MB)
   24h Crashes: $crash_count
   Last Update: $timestamp
EOL

    # Check thresholds and send alerts if needed
    if [[ $memory_usage -gt $MEMORY_THRESHOLD_MB ]]; then
        send_alert "HIGH MEMORY" "Memory usage: ${memory_usage}MB > ${MEMORY_THRESHOLD_MB}MB threshold"
    fi
    
    if [[ $crash_count -gt 10 ]]; then
        send_alert "HIGH CRASH RATE" "24h crashes: $crash_count (may indicate stability issues)"
    fi
}

send_alert() {
    local title="$1"
    local message="$2"
    
    if [[ "$ENABLE_NOTIFICATIONS" == "true" ]]; then
        curl -s -X POST "https://ntfy.sh/${NTFY_TOPIC}" \
            -H "Title: KausalDB Fuzzing Alert: $title" \
            -H "Tags: warning,fuzzing" \
            -d "$message" || echo "Failed to send notification"
    fi
    
    echo "ALERT [$title]: $message" >> "$MONITORING_DIR/alerts.log"
}

# Run monitoring
monitor_performance
EOF
    chmod +x "$MONITORING_DIR/performance_monitor.sh"

    log_info "Monitoring infrastructure created at $MONITORING_DIR"
    log_info "Configuration file: $MONITORING_DIR/config.sh"
    log_info "Service template: $MONITORING_DIR/kausaldb-fuzzing.service"
}

configure_notifications() {
    log_step "Configuring notification system"
    
    read -p "Enter ntfy.sh topic name (or press Enter to skip): " topic
    
    if [[ -n "$topic" ]]; then
        cat > "$NTFY_CONFIG" << EOF
NTFY_TOPIC="$topic"
ENABLE_NOTIFICATIONS=true
EOF
        log_info "Notifications configured for topic: $topic"
        log_info "You can subscribe at: https://ntfy.sh/$topic"
    else
        log_info "Notifications disabled"
    fi
}

start_continuous_fuzzing() {
    local target="$1"
    
    log_step "Starting continuous fuzzing for target: $target"
    
    # Source configuration
    source "$MONITORING_DIR/config.sh" 2>/dev/null || true
    
    # Start performance monitoring in background
    (
        while true; do
            "$MONITORING_DIR/performance_monitor.sh"
            sleep $((PERFORMANCE_CHECK_INTERVAL_MIN * 60))
        done
    ) &
    echo $! > "$MONITORING_DIR/monitor.pid"
    
    log_info "Performance monitoring started (PID: $(cat "$MONITORING_DIR/monitor.pid"))"
    
    # Start main fuzzing process
    log_info "Starting continuous fuzzing..."
    if [[ "$ENABLE_NOTIFICATIONS" == "true" ]]; then
        exec "$SCRIPT_DIR/fuzz.sh" production "$target"
    else
        exec "$SCRIPT_DIR/fuzz.sh" continuous "$target"
    fi
}

stop_fuzzing() {
    log_step "Stopping all fuzzing processes"
    
    # Stop performance monitoring
    if [[ -f "$MONITORING_DIR/monitor.pid" ]]; then
        local monitor_pid=$(cat "$MONITORING_DIR/monitor.pid")
        if kill "$monitor_pid" 2>/dev/null; then
            log_info "Performance monitoring stopped"
        fi
        rm -f "$MONITORING_DIR/monitor.pid"
    fi
    
    # Stop fuzzing processes
    local fuzzing_pids=$(pgrep -f "fuzz.sh.*continuous\|fuzz.sh.*production" || true)
    if [[ -n "$fuzzing_pids" ]]; then
        echo "$fuzzing_pids" | xargs kill
        log_info "Fuzzing processes stopped"
    else
        log_info "No running fuzzing processes found"
    fi
    
    # Generate final report
    generate_final_report
}

show_status() {
    log_step "Fuzzing process status"
    
    echo "Active Processes:"
    ps aux | grep -E "(fuzz\.sh|fuzzing)" | grep -v grep || echo "No active fuzzing processes"
    echo
    
    if [[ -f "$MONITORING_DIR/metrics/latest.txt" ]]; then
        echo "Latest Metrics:"
        cat "$MONITORING_DIR/metrics/latest.txt"
        echo
    fi
    
    echo "Recent Activity:"
    if [[ -f "$FUZZING_LOG" ]]; then
        tail -10 "$FUZZING_LOG" 2>/dev/null || echo "No recent activity"
    else
        echo "No activity log found"
    fi
}

monitor_dashboard() {
    if [[ -x "$MONITORING_DIR/dashboard.sh" ]]; then
        exec "$MONITORING_DIR/dashboard.sh"
    else
        log_error "Dashboard not available. Run 'setup' first."
        exit 1
    fi
}

generate_final_report() {
    local report_file="$MONITORING_DIR/reports/fuzzing_report_$(date +%Y%m%d_%H%M%S).txt"
    
    log_step "Generating final fuzzing report: $(basename "$report_file")"
    
    cat > "$report_file" << EOF
KausalDB Continuous Fuzzing Report
==================================
Generated: $(date)

SUMMARY:
- Duration: $(uptime)
- Total Crashes: $(find ../fuzz_reports -name "crash_*.txt" 2>/dev/null | wc -l)
- Report Size: $(du -sh ../fuzz_reports 2>/dev/null | cut -f1 || echo "0B")

RECENT CRASHES:
$(find ../fuzz_reports -name "crash_*.txt" -mtime -7 2>/dev/null | head -10 | while read -r f; do echo "- $(basename "$f")"; done)

PERFORMANCE METRICS:
$(cat "$MONITORING_DIR/metrics/latest.txt" 2>/dev/null || echo "No metrics available")

SYSTEM RESOURCES:
- Memory Usage: $(ps -A -o rss= | awk '{sum+=$1} END {printf "%.1f MB", sum/1024}')
- Disk Usage: $(df -h . | tail -1 | awk '{print $5}')

RECOMMENDATIONS:
- Review crash reports in fuzz_reports/ directory
- Check performance metrics for any degradation patterns
- Consider expanding fuzzing coverage based on findings

For detailed analysis, see docs/FUZZING_COVERAGE_REPORT.md
EOF

    log_info "Final report saved: $report_file"
}

clean_monitoring() {
    log_step "Cleaning up monitoring infrastructure"
    
    # Stop all processes first
    stop_fuzzing
    
    read -p "Remove all monitoring data? (y/N): " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        rm -rf "$MONITORING_DIR"
        log_info "Monitoring infrastructure removed"
    else
        log_info "Monitoring data preserved"
    fi
}

# Main execution
main() {
    local enable_notifications=false
    local target="all"
    local command=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --notifications)
                enable_notifications=true
                shift
                ;;
            --target)
                target="$2"
                shift 2
                ;;
            --help)
                show_help
                exit 0
                ;;
            setup|start|stop|status|monitor|clean)
                command="$1"
                shift
                ;;
            *)
                if [[ -z "$command" ]]; then
                    command="$1"
                fi
                shift
                ;;
        esac
    done
    
    # Validate target
    case "$target" in
        storage|query|parser|serialization|all)
            ;;
        *)
            log_error "Invalid target: $target"
            exit 1
            ;;
    esac
    
    # Execute command
    case "$command" in
        setup)
            setup_monitoring_infrastructure
            if [[ "$enable_notifications" == "true" ]]; then
                configure_notifications
            fi
            log_info "Production fuzzing setup complete"
            log_info "Start fuzzing: $0 start $target"
            ;;
        start)
            if [[ ! -d "$MONITORING_DIR" ]]; then
                log_error "Monitoring not set up. Run: $0 setup"
                exit 1
            fi
            start_continuous_fuzzing "$target"
            ;;
        stop)
            stop_fuzzing
            ;;
        status)
            show_status
            ;;
        monitor)
            monitor_dashboard
            ;;
        clean)
            clean_monitoring
            ;;
        "")
            log_error "No command specified"
            show_help
            exit 1
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"