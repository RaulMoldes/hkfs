use futures::future::try_join_all;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

pub struct BlockData {
    pub block_id: u32,
    pub path: String,
}

impl BlockData {
    /// Crea una nueva instancia de `BlockData`.
    pub fn new(block_id:u32, path: String) -> Self {
        Self { block_id, path }
    }

    /// Almacena el bloque dividiéndolo en particiones de tamaño fijo.
    ///
    /// # Argumentos
    /// - `partition_size`: Tamaño de cada partición (por ejemplo, 24 bytes).
    /// - `path`: Ruta donde se almacenarán las particiones.
    pub async fn store_block(&self, partition_size: usize, data: Vec<u8>) -> io::Result<()> {
        println!(
            "Almacenando bloque con ID '{}' en particiones de {} bytes.",
            self.block_id, partition_size
        );

        let base_path = Path::new(&self.path);
        fs::create_dir_all(base_path).await?;

        // Iterar y almacenar cada partición de forma concurrente
        let tasks: Vec<_> = data
            .chunks(partition_size)
            .enumerate()
            .map(|(i, chunk)| {
                let partition_file_path = base_path.join(format!("part_{}.blk", i));
                let chunk = chunk.to_vec(); // Clonar el chunk para moverlo al async block
                async move {
                    let mut file = File::create(&partition_file_path).await?;
                    file.write_all(&chunk).await?;
                    println!(
                        "Partición {} almacenada en: {}",
                        i,
                        partition_file_path.display()
                    );
                    Ok::<(), io::Error>(())
                }
            })
            .collect();

        // Ejecutar todas las tareas concurrentemente
        try_join_all(tasks).await?;

        println!("Bloque '{}' almacenado correctamente.", self.block_id);
        Ok(())
    }
    /// Carga un bloque desde el sistema de archivos, reconstruyendo los datos originales.
    pub async fn load_block(&mut self) -> io::Result<Vec<u8>> {
        let base_path = Path::new(&self.path);
        let mut data = Vec::new();
        let mut i = 0;

        if !base_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("El directorio '{}' no existe.", base_path.display()),
            ));
        }

        loop {
            let partition_file_path: PathBuf = base_path.join(format!("part_{}.blk", i));

            if !partition_file_path.exists() {
                break;
            }

            println!("Cargando partición: {}", partition_file_path.display());
            let mut file = File::open(&partition_file_path).await?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).await?;
            data.extend(buffer);
            i += 1;
        }

        if i == 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "No se encontraron particiones para el bloque '{}'.",
                    self.block_id
                ),
            ));
        }

        println!(
            "Bloque '{}' cargado correctamente. Se leyeron {} particiones.",
            self.block_id, i
        );

        Ok(data)
    }
}
