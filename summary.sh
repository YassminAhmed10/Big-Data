

set -euo pipefail

CONTAINER="${1:-cl-run}"
CONTAINER_DIR="/app/pipeline"
HOST_RESULTS="$(dirname "$0")/results"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[summary]${NC} $*"; }
warn()  { echo -e "${YELLOW}[summary]${NC} $*"; }
error() { echo -e "${RED}[summary] ERROR:${NC} $*"; exit 1; }

command -v docker &>/dev/null || error "Docker not found in PATH."

docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER" \
  || error "Container '$CONTAINER' not found."

mkdir -p "$HOST_RESULTS"
info "Saving results to: $HOST_RESULTS"

COPIED=0; MISSED=0
for PATTERN in "*.csv" "*.txt" "*.png"; do
    FILES=$(docker exec "$CONTAINER" bash -c "ls ${CONTAINER_DIR}/${PATTERN} 2>/dev/null || true")
    for FP in $FILES; do
        FN=$(basename "$FP")
        if docker cp "${CONTAINER}:${FP}" "${HOST_RESULTS}/${FN}" 2>/dev/null; then
            info "  ✓ $FN"; COPIED=$((COPIED+1))
        else
            warn "  ✗ $FN"; MISSED=$((MISSED+1))
        fi
    done
done

info "Copied $COPIED file(s)."
[ "$MISSED" -gt 0 ] && warn "$MISSED file(s) failed."

info "Stopping container '$CONTAINER' ..."
docker stop "$CONTAINER" 2>/dev/null && info "  ✓ Stopped." || warn "  Already stopped."
docker rm   "$CONTAINER" 2>/dev/null && info "  ✓ Removed." || warn "  Already removed."

echo ""
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
info "  Results in: $HOST_RESULTS"
ls -lh "$HOST_RESULTS" 2>/dev/null || true
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
info "Done ✓"
