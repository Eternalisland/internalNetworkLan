use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

const MAX_JSON_LINE_BYTES: usize = 8 * 1024;

pub async fn write_json_line<W, T>(write: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize,
{
    let mut result = serde_json::to_vec(value)?;
    result.push(b'\n');
    write.write_all(&result).await?;
    write.flush().await?;
    Ok(())
}

pub async fn read_json_line<R, T>(reader: &mut BufReader<R>) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut line = Vec::new();
    let n = reader.read_until(b'\n', &mut line).await?;

    if n == 0 {
        anyhow::bail!("connection closed");
    }

    if line.len() > MAX_JSON_LINE_BYTES {
        anyhow::bail!("control frame too large: {} bytes", line.len());
    }

    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }

    let msg = serde_json::from_slice::<T>(&line)?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::read_json_line;
    use tokio::io::{AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn rejects_oversized_control_frame() {
        let (mut write, read) = tokio::io::duplex(16 * 1024);
        let payload = format!("\"{}\"\n", "x".repeat(9 * 1024));
        write.write_all(payload.as_bytes()).await.unwrap();
        drop(write);

        let mut reader = BufReader::new(read);
        let error = read_json_line::<_, String>(&mut reader).await.unwrap_err();

        assert!(error.to_string().contains("control frame too large"));
    }
}
