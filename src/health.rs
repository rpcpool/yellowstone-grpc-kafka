use {
    tokio::{
        fs::File,
        io::{self, AsyncWriteExt},
    },
    tracing::info,
};

async fn touch() -> io::Result<()> {
    let mut file = File::create("ping.txt").await?;
    file.write_all(b"ping").await?;
    Ok(())
}

pub async fn ack_ping() {
    info!("ping");
    touch().await.expect("could not touch ping.txt");
}
pub async fn ack_pong() {
    info!("pong");
}
