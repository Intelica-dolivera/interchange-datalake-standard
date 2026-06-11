#!/usr/bin/env bash
# sync-lambdas.sh
# Sincroniza codigo fuente y configuracion de Lambdas desde AWS al repo local.
# Equivalente bash de sync-lambdas.ps1 para entornos Linux/WSL.
#
# Prerequisitos:
#   aws sso login --profile itx-dev
#   export AWS_PROFILE=itx-dev
#
# Uso:
#   ./scripts/sync-lambdas.sh                        # sincroniza todos
#   ./scripts/sync-lambdas.sh -g mc                  # solo Mastercard
#   ./scripts/sync-lambdas.sh -g vi                  # solo Visa
#   ./scripts/sync-lambdas.sh -g general             # solo generales (router, unzip, archive-file)
#   ./scripts/sync-lambdas.sh -l mc-interpreter      # uno especifico

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMP_DIR="/tmp/lambda-sync-$$"
PREFIX="itl-0004-itx-dev-intchg-02-lmbd"
AWS_PROFILE="${AWS_PROFILE:-interchange-dev}"
export AWS_PROFILE

# Verificar dependencias
for cmd in aws curl unzip jq; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: '$cmd' no esta instalado. Instalar con: sudo apt install $cmd" >&2
        exit 1
    fi
done

# Registro de lambdas: sufijo => grupo y directorio local
declare -A LAMBDA_GROUP
declare -A LAMBDA_DIR
declare -a LAMBDA_ORDER

_register() {
    LAMBDA_GROUP["$1"]="$2"
    LAMBDA_DIR["$1"]="$3"
    LAMBDA_ORDER+=("$1")
}

# Generales
_register "router"            "general" "lambdas/router"
_register "unzip"             "general" "lambdas/unzip"
_register "archive-file"      "general" "lambdas/archive-file"
# Visa
_register "vi-transform"      "vi"      "lambdas/visa/transform"
_register "vi-extract"        "vi"      "lambdas/visa/extract"
_register "vi-clean"          "vi"      "lambdas/visa/clean"
_register "vi-store"          "vi"      "lambdas/visa/store"
_register "vi-ardef"          "vi"      "lambdas/visa/ardef"
_register "vi-exchange-rates" "vi"      "lambdas/visa/exchange-rates"
# Mastercard
_register "mc-interpreter"    "mc"      "lambdas/mastercard/interpreter"
_register "mc-transform"      "mc"      "lambdas/mastercard/transform"
_register "mc-extract"        "mc"      "lambdas/mastercard/extract"
_register "mc-clean"          "mc"      "lambdas/mastercard/clean"
_register "mc-store"          "mc"      "lambdas/mastercard/store"
_register "mc-iar"            "mc"      "lambdas/mastercard/iar"
_register "mc-exchange-rates" "mc"      "lambdas/mastercard/exchange-rates"

# Parsear argumentos
GROUP="all"
LAMBDA_FILTER=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -g|--group)
            GROUP="$2"; shift 2 ;;
        -l|--lambda)
            LAMBDA_FILTER="$2"; shift 2 ;;
        *)
            echo "ERROR: argumento desconocido: $1" >&2
            echo "Uso: $0 [-g all|mc|vi|general] [-l <lambda-suffix>]" >&2
            exit 1 ;;
    esac
done

# Validar -l si se paso
if [[ -n "$LAMBDA_FILTER" ]] && [[ -z "${LAMBDA_GROUP[$LAMBDA_FILTER]+_}" ]]; then
    echo "ERROR: Lambda '$LAMBDA_FILTER' no reconocido. Opciones validas:"
    printf '  %s\n' "${LAMBDA_ORDER[@]}"
    exit 1
fi

# Filtrar lista a sincronizar
TO_SYNC=()
for suffix in "${LAMBDA_ORDER[@]}"; do
    if [[ -n "$LAMBDA_FILTER" ]]; then
        [[ "$suffix" == "$LAMBDA_FILTER" ]] && TO_SYNC+=("$suffix")
    elif [[ "$GROUP" == "all" ]] || [[ "${LAMBDA_GROUP[$suffix]}" == "$GROUP" ]]; then
        TO_SYNC+=("$suffix")
    fi
done

if [[ ${#TO_SYNC[@]} -eq 0 ]]; then
    echo "No hay Lambdas para sincronizar con los parametros indicados."
    exit 0
fi

echo "Lambdas a sincronizar: ${#TO_SYNC[@]}"
printf '  %s\n' "${TO_SYNC[@]}"
echo ""

mkdir -p "$TEMP_DIR"
trap 'rm -rf "$TEMP_DIR"' EXIT

OK=0
SKIP=0

for suffix in "${TO_SYNC[@]}"; do
    function_name="${PREFIX}-${suffix}"
    local_dir="${REPO_ROOT}/${LAMBDA_DIR[$suffix]}"
    local_src="${local_dir}/src"
    zip_path="${TEMP_DIR}/${suffix}.zip"
    extract_path="${TEMP_DIR}/${suffix}"

    echo "[$suffix]"

    # 1. Configuracion
    config_json=$(aws lambda get-function-configuration \
        --function-name "$function_name" \
        --output json 2>/dev/null) || true

    if [[ -z "$config_json" ]]; then
        echo "  SKIP - Lambda no encontrado en AWS"
        SKIP=$((SKIP + 1))
        echo ""
        continue
    fi

    mkdir -p "$local_dir"
    echo "$config_json" > "${local_dir}/config.json"
    echo "  config.json actualizado"

    # env-vars.json
    env_vars=$(echo "$config_json" | jq '.Environment.Variables // {}')
    var_count=$(echo "$env_vars" | jq 'keys | length')
    echo "{\"Variables\": ${env_vars}}" | jq '.' > "${local_dir}/env-vars.json"
    echo "  env-vars.json actualizado ($var_count vars)"

    # 2. Codigo fuente
    url=$(aws lambda get-function \
        --function-name "$function_name" \
        --query "Code.Location" \
        --output text 2>/dev/null) || true

    if [[ -z "$url" ]] || [[ "$url" == "None" ]]; then
        echo "  WARN - No se pudo obtener URL del codigo"
        SKIP=$((SKIP + 1))
        echo ""
        continue
    fi

    curl -s -o "$zip_path" "$url"
    size_kb=$(du -k "$zip_path" | cut -f1)
    echo "  ZIP: ${size_kb} KB"

    rm -rf "$extract_path"
    mkdir -p "$extract_path"
    unzip -q "$zip_path" -d "$extract_path"

    mkdir -p "$local_src"
    cp -rf "${extract_path}/." "$local_src/"
    echo "  src/ actualizado"
    OK=$((OK + 1))
    echo ""
done

echo "========== Resumen =========="
printf '%-20s %s\n' "Lambda" "Estado"
printf '%-20s %s\n' "------" "------"
for suffix in "${TO_SYNC[@]}"; do
    if [[ -f "${REPO_ROOT}/${LAMBDA_DIR[$suffix]}/src/handler.py" ]]; then
        printf '%-20s %s\n' "$suffix" "OK"
    else
        printf '%-20s %s\n' "$suffix" "SKIP/ERROR"
    fi
done
echo ""
if [[ $SKIP -eq 0 ]]; then
    echo "Completado: $OK OK"
else
    echo "Completado: $OK OK, $SKIP con advertencias/errores"
fi
