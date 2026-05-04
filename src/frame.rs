
use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncRead,
                 AsyncWrite,
                AsyncWriteExt, BufReader};

pub async fn write_json_line<W, T>(write : &mut W, value : &T) -> Result<()>
where
    W : AsyncWrite + Unpin,
    T : serde::Serialize,
{
    let mut result = serde_json::to_vec(value)?;
    result.push(b'\n');
    write.write_all(&result).await?;
    write.flush().await?;
    Ok(())
}


pub async fn read_json_line<R, T>(reader : &mut BufReader<R>)  -> Result<T>
where R : AsyncRead + Unpin,
      T : serde::de::DeserializeOwned
{

    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;

    if n == 0 {
        println!("connection closed");
    }
    let msg = serde_json::from_str::<T>(&line)?;
    Ok(msg)
}