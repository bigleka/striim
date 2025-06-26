# este arquivo trabalha em conjunto com o gerador de arquivos tql
# para fazer o upload dos arquivos tql para seu servidor striim


# pré requisito para a resposta do request do token.
#sudo apt install jq

#!/bin/bash

# Configurações
USERNAME="admin"
PASSWORD="change_the_password" # replace to your password
STRIIM_HOST="http://IP_ADDRESS:9080" # address of the striim server
AUTH_ENDPOINT="/security/authenticate"
API_UPLOAD="/api/v2/tungsten"
TQL_DIR="folder where the tql files are"  # replace to the tql folder

# Limite de processos paralelos
MAX_PARALLEL=5 # chace to increase or decrease the parallelism

echo "Gerando token de autenticação..."
TOKEN=$(curl -s -X POST -d "username=$USERNAME&password=$PASSWORD" "$STRIIM_HOST$AUTH_ENDPOINT" | jq -r '.token')

# Verifica se o token foi obtido
if [[ "$TOKEN" == "null" || -z "$TOKEN" ]]; then
    echo "Erro: não foi possível obter o token. Verifique suas credenciais."
    exit 1
fi

echo "Token obtido: $TOKEN"
echo

# Função para upload de um arquivo .tql
upload_file() {
    local file="$1"
    echo "Enviando arquivo: $file"
    response=$(curl -s -X POST "$STRIIM_HOST$API_UPLOAD" \
        -H "authorization: STRIIM-TOKEN $TOKEN" \
        -H "content-type: text/plain" \
        --data-binary @"$file")
    echo "Resposta do arquivo '$file':"
    echo "$response"
    echo
}

# Array para armazenar os PIDs dos jobs em background
pids=()

# Itera sobre todos os arquivos .tql no diretório (não recursivo)
shopt -s nullglob
for file in "$TQL_DIR"/*.tql; do
    # Se não houver nenhum arquivo, sai do loop
    if [[ ! -f "$file" ]]; then
        echo "Nenhum arquivo .tql encontrado em $TQL_DIR."
        break
    fi

    # Inicia upload em background
    upload_file "$file" &
    pids+=( "$!" )

    # Se o número de processos ativos atingir o limite, espera que algum finalize.
    while [ "${#pids[@]}" -ge "$MAX_PARALLEL" ]; do
        # Verifica cada PID; remove os que já finalizaram.
        for i in "${!pids[@]}"; do
            if ! kill -0 "${pids[i]}" 2>/dev/null; then
                unset 'pids[i]'
            fi
        done
        # Compacta o array
        pids=("${pids[@]}")
        sleep 1
    done
done

# Aguarda a finalização de todos os processos em background
wait

echo "Upload concluído para todos os arquivos."
