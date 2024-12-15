#!/bin/bash
cd hkfs/

# Verificar si se pasó un argumento
if [ $# -ne 1 ]; then
    echo "Use: $0 <number_of_datanodes> <replication_factor>"
    exit 1
fi

# Número de procesos a levantar
num_procesos=$1

## Factor de replicación
replication_factor=$2

# Array  para almacenar los PIDs de los procesos
pids=()

# Función para matar todos los procesos
pkill_all() {
    echo "Startig graceful shutdown mechanism..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    echo "Todos los procesos han sido terminados."
    exit 0
}

# Función para simular un proceso (puedes reemplazarlo con un comando real)
start_process() {
    local pid=$1
    echo "Datanode $pid started."
    cargo run --bin datanode &
    pids+=($!)   # Almacena el PID del proceso en el arreglo
}


# Capturar señales como Ctrl + C (SIGINT) para terminar procesos
trap 'pkill_all' SIGINT

# Verificar si el argumento es un número válido
if ! [[ "$num_procesos" =~ ^[0-9]+$ ]]; then
    echo "Error: The argument must be a valid number."
    exit 1
fi
## Iniciar el namenode
## Asignar el pid 0 al namenode uy guardar el pid en el arreglo
echo "Number of datanodes: $num_procesos"
echo "Starting namenode..."
cargo run --bin namenode $replication_factor &
pids[0]=$!

echo "Starting  $num_procesos datanodes.."

# Levantar procesos secuencialmente
for i in $(seq 1 $num_procesos); do
    start_process $i
done

echo "All datanodes started."

wait
