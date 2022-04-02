use erl_dist::epmd::{EpmdClient, NodeEntry};
use erl_dist::handshake::{ClientSideHandshake, HandshakeStatus};
use erl_dist::message::{self};
use erl_dist::node::{Creation, LocalNode, NodeName, PeerNode};
use erl_dist::DistributionFlags;
use smol::net::TcpStream;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(
        "the server only supports the distribution protocol version 5 while this crate requires 6"
    )]
    TooOldDistributionProtocolVersion,

    #[error("unexpected handshake status: {status:?}")]
    UnexpectedHandshakeStatus { status: HandshakeStatus },

    #[error("no such Erlang node: {name}")]
    NodeNodeFound { name: NodeName },

    #[error(transparent)]
    NodeNameError(#[from] erl_dist::node::NodeNameError),

    #[error(transparent)]
    EpmdError(#[from] erl_dist::epmd::EpmdError),

    #[error(transparent)]
    HandshakeError(#[from] erl_dist::handshake::HandshakeError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct RpcClient {
    tx: message::Sender<TcpStream>,
    rx: message::Receiver<TcpStream>,
    local_node: LocalNode,
    peer_node: PeerNode,
}

impl RpcClient {
    pub async fn connect(server_node_name: &str, cookie: &str) -> Result<Self, Error> {
        let server_node_name: NodeName = server_node_name.parse()?;
        let server_node_entry = get_node_entry(&server_node_name).await?;
        if server_node_entry.highest_version < 6 {
            return Err(Error::TooOldDistributionProtocolVersion);
        }

        let tentative_name = "nonode@localhost";
        let mut local_node = LocalNode::new(tentative_name.parse()?, Creation::random());
        local_node.flags |= DistributionFlags::NAME_ME;

        let connection =
            TcpStream::connect((server_node_name.host(), server_node_entry.port)).await?;
        let mut handshake = ClientSideHandshake::new(connection, local_node.clone(), cookie);
        let status = handshake.execute_send_name(6).await?;
        if let HandshakeStatus::Named { name, creation } = status {
            local_node.name = NodeName::new(&name, local_node.name.host())?;
            local_node.creation = creation;
            let (connection, peer_node) = handshake.execute_rest(true).await?;
            let (tx, rx) = message::channel(connection, local_node.flags & peer_node.flags);
            Ok(Self {
                tx,
                rx,
                local_node,
                peer_node,
            })
        } else {
            return Err(Error::UnexpectedHandshakeStatus { status });
        }
    }
}

async fn get_node_entry(node_name: &NodeName) -> Result<NodeEntry, Error> {
    let connection =
        TcpStream::connect((node_name.host(), erl_dist::epmd::DEFAULT_EPMD_PORT)).await?;
    let client = EpmdClient::new(connection);
    if let Some(node) = client.get_node(node_name.name()).await? {
        Ok(node)
    } else {
        Err(Error::NodeNodeFound {
            name: node_name.clone(),
        })
    }
}
