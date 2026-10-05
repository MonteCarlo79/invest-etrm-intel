#!/usr/bin/env bash
# Run the LLM stages of power-academy as a one-off Fargate task (Anthropic is geo-blocked from the Mac).
# Usage: bash scripts/run_cloud.sh pilot|full|pull|cleanup
set -euo pipefail
export AWS_PAGER=""
R=ap-southeast-1
CLUSTER=bess-platform-cluster
SVC=bess-platform-deal-structurer-svc
TD=arn:aws:ecs:$R:319383842493:task-definition/bess-platform-deal-structurer:33
CONTAINER=deal-structurer
B=bess-uploader-data-chen-singp-2026   # only bucket the task role can access
P=power-academy
PILOT_IDS=clewlow,plant_hedging_and_trading_strategies_kyos_20110530,power_battery
HERE="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PY:-$HOME/.venvs/bess-platform/bin/python}"

launch() {  # $1 = shell command to run inside the container after unpacking the bundle
  current=$(aws ecs describe-services --cluster $CLUSTER --services $SVC --region $R \
            --query 'services[0].taskDefinition' --output text)
  [ "$current" = "$TD" ] || { echo "ABORT: service is on $current, script pins $TD"; exit 1; }
  CMD="pip install -q pyyaml pymupdf python-pptx python-docx markdown pandas sqlalchemy && \
python -c \"import boto3;boto3.client('s3').download_file('$B','$P/bundle.tar.gz','/tmp/b.tgz')\" && \
mkdir -p /tmp/work && tar xzf /tmp/b.tgz -C /tmp/work && cd /tmp/work/power-academy && $1"
  python3 - "$CMD" "$CONTAINER" <<'PYEOF'
import json, sys
json.dump({"containerOverrides": [{"name": sys.argv[2], "command": ["sh", "-c", sys.argv[1]]}]},
          open("/tmp/pa_overrides.json", "w"))
PYEOF
  net=$(aws ecs describe-services --cluster $CLUSTER --services $SVC --region $R \
        --query 'services[0].networkConfiguration' --output json)
  echo "$net" > /tmp/pa_net.json
  arn=$(aws ecs run-task --cluster $CLUSTER --launch-type FARGATE --region $R --task-definition "$TD" \
        --network-configuration file:///tmp/pa_net.json --overrides file:///tmp/pa_overrides.json \
        --started-by power-academy --query 'tasks[0].taskArn' --output text)
  echo "task: $arn"
  while :; do
    st=$(aws ecs describe-tasks --cluster $CLUSTER --tasks "$arn" --region $R --query 'tasks[0].lastStatus' --output text)
    echo "  status: $st"; [ "$st" = STOPPED ] && break; sleep 20
  done
  aws ecs describe-tasks --cluster $CLUSTER --tasks "$arn" --region $R \
    --query 'tasks[0].containers[0].{exit:exitCode,reason:reason}' --output json
  opts=$(aws ecs describe-task-definition --task-definition "$TD" --region $R \
         --query 'taskDefinition.containerDefinitions[0].logConfiguration.options' --output json)
  grp=$(echo "$opts" | python3 -c 'import json,sys;print(json.load(sys.stdin)["awslogs-group"])')
  pre=$(echo "$opts" | python3 -c 'import json,sys;print(json.load(sys.stdin)["awslogs-stream-prefix"])')
  echo "--- last log lines ---"
  aws logs get-log-events --log-group-name "$grp" --log-stream-name "$pre/$CONTAINER/${arn##*/}" \
    --region $R --limit 40 --query 'events[].message' --output text || true
}

case "${1:-}" in
  pilot)   launch "python -m academy.cli outline --only $PILOT_IDS && python -m academy.cli push-results --bucket $B --prefix $P" ;;
  full)    (cd "$HERE" && "$PY" -m academy.cli bundle --bucket $B --prefix $P)
           launch "S=0; python -m academy.cli outline || S=1; python -m academy.cli push-results --bucket $B --prefix $P; python -m academy.cli coverage || S=1; python -m academy.cli push-results --bucket $B --prefix $P; python -m academy.cli syllabus || S=1; python -m academy.cli push-results --bucket $B --prefix $P; exit \$S" ;;
  translate)
           (cd "$HERE" && "$PY" -m academy.cli bundle --bucket $B --prefix $P)
           IDS="spark_dark_spread_fundamentals heat_rate_and_plant_parameters intrinsic_vs_extrinsic_value stochastic_price_processes_for_power_and_fuel spread_option_pricing_models real_options_framework_for_generation_assets monte_carlo_and_lsm_for_generation_valuation piecewise_replication_and_ccgt_intrinsic_modelling power_price_spike_models_and_option_valuation ccgt_investment_option_and_optimal_timing tolling_agreement_structure_and_valuation dispatch_optimisation_and_delta_hedging power_plant_economics_and_dispatch forward_curve_structure_and_products profit_at_risk_and_hedging_objective baseload_peak_offpeak_delta_decomposition delta_sensitivity_and_hedge_volume gradual_linear_and_benchmark_hedging_strategies rolling_intrinsic_strategy static_vs_dynamic_hedging option_greeks_for_power_derivatives hedging_under_incomplete_markets_and_spike_dynamics ppa_structures_and_route_to_market tolling_and_asset_backed_trading"
           launch "S=0; for id in $IDS; do python -m academy.cli concept translate \\$id || S=1; done; python -m academy.cli push-results --bucket $B --prefix $P; exit \\$S" ;;
  pull)    (cd "$HERE" && "$PY" -m academy.cli pull-results --bucket $B --prefix $P) ;;
  cleanup) aws s3 rm "s3://$B/$P/" --recursive ;;
  *) echo "usage: $0 pilot|full|pull|cleanup"; exit 2 ;;
esac
