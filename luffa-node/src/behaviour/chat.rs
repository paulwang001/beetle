use async_trait::async_trait;
use futures::{prelude::*, AsyncWriteExt};
use libp2p::core::{
    upgrade::{read_length_prefixed, write_length_prefixed},
    PeerId, ProtocolName,
};
use std::{io, iter};
use libp2p::request_response::RequestResponseCodec;
// use libp2p::request_response::
#[derive(Debug, Clone)]
pub struct ChatProtocol();
#[derive(Clone)]
pub struct ChatCodec();
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request(pub Vec<u8>);

impl Request {
    pub fn data(&self) -> &Vec<u8>{
        self.0.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response(pub Vec<u8>);

impl ProtocolName for ChatProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/chat/0.1.0".as_bytes()
    }
}

#[async_trait]
impl RequestResponseCodec for ChatCodec {
    type Protocol = ChatProtocol;
    type Request = Request;
    type Response = Response;

    async fn read_request<T>(&mut self, _: &ChatProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 4 * 1024 * 1024).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        Ok(Request(vec))
    }

    async fn read_response<T>(&mut self, _: &ChatProtocol, io: &mut T) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 4 * 1024 * 1024).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        Ok(Response(vec))
    }

    async fn write_request<T>(
        &mut self,
        _: &ChatProtocol,
        io: &mut T,
        Request(data): Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_length_prefixed(io, data).await?;
        io.close().await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &ChatProtocol,
        io: &mut T,
        Response(data): Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_length_prefixed(io, data).await?;
        io.close().await?;

        Ok(())
    }
}