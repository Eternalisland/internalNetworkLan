// 更新内容：增加 UDP 隧道封装帧，支持客户端注册 UDP 通道和多公网来源 UDP 数据复用。
use anyhow::{Result, anyhow};

const UDP_MAGIC: &[u8; 8] = b"RTUNUDP1";
const UDP_TYPE_REGISTER: u8 = 1;
const UDP_TYPE_DATA: u8 = 2;

#[derive(Debug)]
pub enum UdpPacket {
    Register {
        session_id: u64,
        proxy_port: u16,
        work_token: String,
    },
    Data {
        session_id: u64,
        proxy_port: u16,
        conn_id: u64,
        payload: Vec<u8>,
    },
}

pub fn build_udp_register(session_id: u64, proxy_port: u16, work_token: &str) -> Vec<u8> {
    let token = work_token.as_bytes();
    let token_len = token.len().min(u16::MAX as usize);
    let mut frame = Vec::with_capacity(8 + 1 + 8 + 2 + 2 + token_len);
    frame.extend_from_slice(UDP_MAGIC);
    frame.push(UDP_TYPE_REGISTER);
    frame.extend_from_slice(&session_id.to_be_bytes());
    frame.extend_from_slice(&proxy_port.to_be_bytes());
    frame.extend_from_slice(&(token_len as u16).to_be_bytes());
    frame.extend_from_slice(&token[..token_len]);
    frame
}

pub fn build_udp_data(session_id: u64, proxy_port: u16, conn_id: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8 + 1 + 8 + 2 + 8 + payload.len());
    frame.extend_from_slice(UDP_MAGIC);
    frame.push(UDP_TYPE_DATA);
    frame.extend_from_slice(&session_id.to_be_bytes());
    frame.extend_from_slice(&proxy_port.to_be_bytes());
    frame.extend_from_slice(&conn_id.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn parse_udp_packet(bytes: &[u8]) -> Result<Option<UdpPacket>> {
    if bytes.len() < UDP_MAGIC.len() || &bytes[..UDP_MAGIC.len()] != UDP_MAGIC {
        return Ok(None);
    }

    let packet_type = *bytes
        .get(UDP_MAGIC.len())
        .ok_or_else(|| anyhow!("udp tunnel frame missing type"))?;
    let mut cursor = UDP_MAGIC.len() + 1;

    match packet_type {
        UDP_TYPE_REGISTER => {
            let session_id = read_u64(bytes, &mut cursor)?;
            let proxy_port = read_u16(bytes, &mut cursor)?;
            let token_len = read_u16(bytes, &mut cursor)? as usize;
            if bytes.len() < cursor + token_len {
                anyhow::bail!("udp register token truncated");
            }
            let work_token = String::from_utf8(bytes[cursor..cursor + token_len].to_vec())?;
            Ok(Some(UdpPacket::Register {
                session_id,
                proxy_port,
                work_token,
            }))
        }
        UDP_TYPE_DATA => {
            let session_id = read_u64(bytes, &mut cursor)?;
            let proxy_port = read_u16(bytes, &mut cursor)?;
            let conn_id = read_u64(bytes, &mut cursor)?;
            Ok(Some(UdpPacket::Data {
                session_id,
                proxy_port,
                conn_id,
                payload: bytes[cursor..].to_vec(),
            }))
        }
        _ => Err(anyhow!("unknown udp tunnel frame type {}", packet_type)),
    }
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    if bytes.len() < *cursor + 2 {
        anyhow::bail!("udp tunnel frame truncated while reading u16");
    }
    let value = u16::from_be_bytes([bytes[*cursor], bytes[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    if bytes.len() < *cursor + 8 {
        anyhow::bail!("udp tunnel frame truncated while reading u64");
    }
    let value = u64::from_be_bytes(bytes[*cursor..*cursor + 8].try_into()?);
    *cursor += 8;
    Ok(value)
}
