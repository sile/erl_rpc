//! Erlang RPC Client.
//!
//! # Examples
//!
//! ```no_run
//! # fn main() -> anyhow::Result<()> {
//! smol::block_on(async {
//!     // Connect to an Erlang node.
//!     let erlang_node = "foo@localhost";
//!     let cookie = "cookie-value";
//!     let client = erl_rpc::RpcClient::connect(erlang_node, cookie).await?;
//!     let mut handle = client.handle();
//!
//!     // Run the RPC client as a background task.
//!     smol::spawn(async {
//!         if let Err(e) = client.run().await {
//!             eprintln!("RpcClient Error: {}", e);
//!         }
//!     }).detach();
//!
//!     // Execute an RPC: `erlang:processes/0`
//!     let result = handle
//!         .call("erlang".into(), "processes".into(), erl_dist::term::List::nil())
//!         .await?;
//!     println!("{}", result);
//!     Ok(())
//! })
//! # }
//! ```
#![warn(missing_docs)]
use erl_dist::epmd::{EpmdClient, NodeEntry};
use erl_dist::handshake::{ClientSideHandshake, HandshakeStatus};
use erl_dist::message::{self, Message};
use erl_dist::node::{Creation, LocalNode, NodeName, PeerNode};
use erl_dist::term::{Atom, FixInteger, List, Mfa, Pid, PidOrAtom, Reference, Term};
use erl_dist::DistributionFlags;
use futures::channel::{mpsc, oneshot};
use futures::FutureExt;
use smol::net::TcpStream;
use std::collections::HashMap;

/// Possible errors during [`RpcClient::connect`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum ConnectError {
    #[error(
        "the server only supports the distribution protocol version 5 while the client requires 6"
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

/// Possible errors during [`RpcClientHandle::call`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum CallError {
    #[error("received an error response: {reason}")]
    ErrorResponse { reason: Term },

    #[error("send buffer is full")]
    Full,

    #[error("RpcClient has been terminated")]
    Terminated,
}

/// RPC client.
#[derive(Debug)]
pub struct RpcClient {
    msg_rx: Option<message::Receiver<TcpStream>>,
    msg_tx: message::Sender<TcpStream>,
    req_rx: mpsc::Receiver<Request>,
    req_tx: Option<mpsc::Sender<Request>>,
    local_node: LocalNode,
    peer_node: PeerNode,
    ongoing_reqs: HashMap<Reference, oneshot::Sender<Term>>,
}

impl RpcClient {
    /// Connects to a given Erlang node.
    pub async fn connect(server_node_name: &str, cookie: &str) -> Result<Self, ConnectError> {
        let server_node_name: NodeName = server_node_name.parse()?;
        let server_node_entry = get_node_entry(&server_node_name).await?;
        if server_node_entry.highest_version < 6 {
            return Err(ConnectError::TooOldDistributionProtocolVersion);
        }

        let tentative_name = "nonode@localhost";
        let mut local_node = LocalNode::new(tentative_name.parse()?, Creation::random());
        local_node.flags |= DistributionFlags::NAME_ME;
        local_node.flags |= DistributionFlags::SPAWN;
        local_node.flags |= DistributionFlags::DIST_MONITOR;
        local_node.flags |= DistributionFlags::DIST_MONITOR_NAME;

        let connection =
            TcpStream::connect((server_node_name.host(), server_node_entry.port)).await?;
        let mut handshake = ClientSideHandshake::new(connection, local_node.clone(), cookie);
        let status = handshake.execute_send_name(6).await?;
        if let HandshakeStatus::Named { name, creation } = status {
            local_node.name = NodeName::new(&name, local_node.name.host())?;
            local_node.creation = creation;
            let (connection, peer_node) = handshake.execute_rest(true).await?;
            let (msg_tx, msg_rx) = message::channel(connection, local_node.flags & peer_node.flags);
            let (req_tx, req_rx) = mpsc::channel(1024); // TODO: Remove hard-coding
            Ok(Self {
                msg_tx,
                msg_rx: Some(msg_rx),
                req_rx,
                req_tx: Some(req_tx),
                local_node,
                peer_node,
                ongoing_reqs: HashMap::new(),
            })
        } else {
            Err(ConnectError::UnexpectedHandshakeStatus { status })
        }
    }

    /// Returns a handle of this client to request RPCs.
    pub fn handle(&self) -> RpcClientHandle {
        RpcClientHandle {
            req_tx: self.req_tx.clone().take().expect("unreachable"),
        }
    }

    /// Returns the local node information.
    pub fn local_node(&self) -> &LocalNode {
        &self.local_node
    }

    /// Returns the peer node information.
    pub fn peer_ndoe(&self) -> &PeerNode {
        &self.peer_node
    }

    /// Runs a loop to handle RPC.
    pub async fn run(mut self) -> Result<(), RunError> {
        self.req_tx = None;

        let msg_rx = self.msg_rx.take().expect("unreachable");
        let mut msg_rx_fut = msg_rx.recv_owned().boxed();

        let tick_interval = std::time::Duration::from_secs(30);
        let mut tick_timer = smol::Timer::after(tick_interval);

        loop {
            if smol::future::poll_once(&mut tick_timer).await.is_some() {
                self.msg_tx.send(Message::Tick).await?;
                tick_timer.set_after(tick_interval);
            }

            match smol::future::poll_once(&mut msg_rx_fut).await {
                Some(Ok((msg, msg_rx))) => {
                    msg_rx_fut = msg_rx.recv_owned().boxed();
                    self.handle_msg(msg).await?;
                    continue;
                }
                Some(Err(e)) => {
                    return Err(e.into());
                }
                None => {}
            }

            match self.req_rx.try_next() {
                Ok(Some(req)) => {
                    self.handle_req(req).await?;
                    continue;
                }
                Ok(None) => {
                    break;
                }
                Err(_) => {}
            }

            let sleep_duration = std::time::Duration::from_millis(1);
            smol::Timer::after(sleep_duration).await;
        }
        Ok(())
    }

    async fn handle_req(&mut self, req: Request) -> Result<(), RunError> {
        let req_id = self.make_ref();
        let spawn_request = Message::spawn_request(
            req_id.clone(),
            self.pid(),
            self.pid(),
            Mfa {
                module: "erpc".into(),
                function: "execute_call".into(),
                arity: FixInteger::from(4),
            },
            List::from(vec![Atom::from("monitor").into()]),
            List::from(vec![
                self.make_ref().into(),
                req.mfargs.module.into(),
                req.mfargs.function.into(),
                req.mfargs.args.into(),
            ]),
        );
        self.msg_tx.send(spawn_request).await?;
        self.ongoing_reqs.insert(req_id, req.reply_tx);
        Ok(())
    }

    async fn handle_msg(&mut self, msg: Message) -> Result<(), RunError> {
        match msg {
            Message::Tick => Ok(()),
            Message::SpawnReply(msg) => self.handle_spawn_reply(msg).await,
            Message::MonitorPExit(msg) => self.handle_monitor_p_exit(msg).await,
            _ => Err(RunError::UnexpectedMessage { message: msg }),
        }
    }

    async fn handle_spawn_reply(&mut self, msg: message::SpawnReply) -> Result<(), RunError> {
        if let PidOrAtom::Atom(reason) = msg.result {
            Err(RunError::SpawnRequestError {
                reason: reason.name,
            })
        } else {
            Ok(())
        }
    }

    async fn handle_monitor_p_exit(&mut self, msg: message::MonitorPExit) -> Result<(), RunError> {
        if let Some(reply_tx) = self.ongoing_reqs.remove(&msg.reference) {
            let _ = reply_tx.send(msg.reason);
            Ok(())
        } else {
            Err(RunError::UnexpectedResponse { message: msg })
        }
    }

    fn node(&self) -> Atom {
        Atom::from(self.local_node.name.to_string())
    }

    fn pid(&self) -> Pid {
        Pid::new(self.node(), 0, 0, self.local_node.creation.get())
    }

    fn make_ref(&self) -> Reference {
        Reference {
            node: self.node(),
            id: vec![rand::random(), rand::random(), rand::random()],
            creation: self.local_node.creation.get(),
        }
    }
}

/// Pissible errors during [`RpcClient::run()`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum RunError {
    #[error("failed to execute `spawn_request` on the target node: {reason}")]
    SpawnRequestError { reason: String },

    #[error("received an unexpected message: {message:?}")]
    UnexpectedMessage { message: Message },

    #[error("received an RPC response without associating request: {message:?}")]
    UnexpectedResponse { message: message::MonitorPExit },

    #[error(transparent)]
    MessageSendError(#[from] erl_dist::message::SendError),

    #[error(transparent)]
    MessageRecvError(#[from] erl_dist::message::RecvError),
}

/// Handle of [`RpcClient`].
#[derive(Debug, Clone)]
pub struct RpcClientHandle {
    req_tx: mpsc::Sender<Request>,
}

impl RpcClientHandle {
    /// Request an RPC.
    pub async fn call(
        &mut self,
        module: Atom,
        function: Atom,
        args: List,
    ) -> Result<Term, CallError> {
        let mfargs = MFArgs {
            module,
            function,
            args,
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        let req = Request { mfargs, reply_tx };
        let res = if let Err(e) = self.req_tx.try_send(req) {
            if e.is_disconnected() {
                return Err(CallError::Terminated);
            } else {
                debug_assert!(e.is_full());
                return Err(CallError::Full);
            }
        } else {
            reply_rx.await.map_err(|_| CallError::Terminated)?
        };
        if let Term::Tuple(mut res) = res {
            let mut ok = false;
            if res.elements.len() == 3 {
                if let Term::Atom(kind) = &res.elements[1] {
                    if kind.name == "return" {
                        ok = true;
                    }
                }
            }
            if ok {
                let value = std::mem::replace(&mut res.elements[2], List::nil().into());
                Ok(value)
            } else {
                Err(CallError::ErrorResponse { reason: res.into() })
            }
        } else {
            Err(CallError::ErrorResponse { reason: res })
        }
    }
}

#[derive(Debug)]
struct MFArgs {
    module: Atom,
    function: Atom,
    args: List,
}

#[derive(Debug)]
struct Request {
    mfargs: MFArgs,
    reply_tx: oneshot::Sender<Term>,
}

async fn get_node_entry(node_name: &NodeName) -> Result<NodeEntry, ConnectError> {
    let connection =
        TcpStream::connect((node_name.host(), erl_dist::epmd::DEFAULT_EPMD_PORT)).await?;
    let client = EpmdClient::new(connection);
    if let Some(node) = client.get_node(node_name.name()).await? {
        Ok(node)
    } else {
        Err(ConnectError::NodeNodeFound {
            name: node_name.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command};

    const COOKIE: &str = "test-cookie";

    #[derive(Debug)]
    struct TestErlangNode {
        child: Child,
    }

    impl TestErlangNode {
        async fn new(name: &str) -> anyhow::Result<Self> {
            let child = Command::new("erl")
                .args(&["-sname", name, "-noshell", "-setcookie", COOKIE])
                .spawn()?;
            let start = std::time::Instant::now();
            loop {
                if let Ok(client) = try_epmd_client().await {
                    if client.get_node(name).await?.is_some() {
                        break;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(500));
                if start.elapsed() > std::time::Duration::from_secs(10) {
                    break;
                }
            }
            Ok(Self { child })
        }
    }

    impl Drop for TestErlangNode {
        fn drop(&mut self) {
            let _ = self.child.kill();
        }
    }

    async fn try_epmd_client() -> anyhow::Result<erl_dist::epmd::EpmdClient<smol::net::TcpStream>> {
        let client =
            smol::net::TcpStream::connect(("127.0.0.1", erl_dist::epmd::DEFAULT_EPMD_PORT))
                .await
                .map(erl_dist::epmd::EpmdClient::new)?;
        Ok(client)
    }

    #[test]
    fn it_works() {
        smol::block_on(async {
            let server = TestErlangNode::new("erl_rpc_test").await.unwrap();

            let client = RpcClient::connect("erl_rpc_test@localhost", COOKIE)
                .await
                .unwrap();
            let mut handle = client.handle();

            smol::spawn(async {
                if let Err(e) = client.run().await {
                    eprintln!("RpcClient Error: {}", e);
                }
            })
            .detach();

            handle
                .call(
                    "erlang".into(),
                    "processes".into(),
                    erl_dist::term::List::nil(),
                )
                .await
                .unwrap();

            std::mem::drop(server);
        });
    }
}
