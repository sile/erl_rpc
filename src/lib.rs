use erl_dist::epmd::{EpmdClient, NodeEntry};
use erl_dist::handshake::{ClientSideHandshake, HandshakeStatus};
use erl_dist::message::{self, Message};
use erl_dist::node::{Creation, LocalNode, NodeName, PeerNode};
use erl_dist::term::{Atom, FixInteger, List, Mfa, Pid, Reference, Term, Tuple};
use erl_dist::DistributionFlags;
use futures::channel::{mpsc, oneshot};
use futures::FutureExt;
use smol::net::TcpStream;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CallError {
    #[error("send buffer is full")]
    Full,

    #[error("RpcClient has been terminated")]
    Terminated,
}

#[derive(Debug)]
pub struct RpcClient {
    msg_rx: Option<message::Receiver<TcpStream>>,
    msg_tx: message::Sender<TcpStream>,
    req_rx: mpsc::Receiver<Request>,
    req_tx: Option<mpsc::Sender<Request>>,
    local_node: LocalNode,
    peer_node: PeerNode,
    ongoing_reqs: HashMap<Vec<u32>, oneshot::Sender<Term>>,
}

impl RpcClient {
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
            return Err(ConnectError::UnexpectedHandshakeStatus { status });
        }
    }

    pub fn handle(&self) -> RpcClientHandle {
        RpcClientHandle {
            req_tx: self.req_tx.clone().take().expect("unreachable"),
        }
    }

    pub fn local_node(&self) -> &LocalNode {
        &self.local_node
    }

    pub fn peer_ndoe(&self) -> &PeerNode {
        &self.peer_node
    }

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
        let res_id = self.make_ref();
        let spawn_request = Message::spawn_request(
            req_id,
            self.pid(),
            self.pid(),
            Mfa {
                module: "erpc".into(),
                function: "execute_call".into(),
                arity: FixInteger::from(4),
            },
            List::from(vec![
                Tuple::from(vec![
                    Atom::from("reply").into(),
                    Atom::from("error_only").into(),
                ])
                .into(),
                Atom::from("monitor").into(),
            ]),
            List::from(vec![
                res_id.clone().into(),
                req.mfargs.module.into(),
                req.mfargs.function.into(),
                req.mfargs.args.into(),
            ]),
        );
        self.msg_tx.send(spawn_request).await?;
        self.ongoing_reqs.insert(res_id.id.clone(), req.reply_tx);
        // Res = make_ref(),
        // ReqId = spawn_request(N, ?MODULE, execute_call, [Res, M, F, A],
        //                       [{reply, error_only}, monitor]),
        // receive
        // {spawn_reply, ReqId, error, Reason} ->
        //     result(spawn_reply, ReqId, Res, Reason);
        // {'DOWN', ReqId, process, _Pid, Reason} ->
        //     result(down, ReqId, Res, Reason)
        //     after T ->
        //     result(timeout, ReqId, Res, undefined)
        //     end;
        Ok(())
    }

    async fn handle_msg(&mut self, msg: Message) -> Result<(), RunError> {
        dbg!(msg);
        todo!()
    }

    fn node(&self) -> Atom {
        Atom::from(self.local_node.name.to_string())
    }

    fn pid(&self) -> Pid {
        Pid::new(self.node(), 0, 0, self.local_node.creation.get())
    }

    fn make_ref(&self) -> Reference {
        // TDOO:
        Reference {
            node: self.node(),
            id: vec![0, rand::random(), rand::random()], //vec![rand::random(), rand::random(), rand::random()],
            creation: 0,                                 // self.local_node.creation.get() as u8,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    MessageSendError(#[from] erl_dist::message::SendError),

    #[error(transparent)]
    MessageRecvError(#[from] erl_dist::message::RecvError),
}

#[derive(Debug, Clone)]
pub struct RpcClientHandle {
    req_tx: mpsc::Sender<Request>,
}

impl RpcClientHandle {
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
        if let Err(e) = self.req_tx.try_send(req) {
            if e.is_disconnected() {
                return Err(CallError::Terminated);
            } else {
                debug_assert!(e.is_full());
                return Err(CallError::Full);
            }
        } else {
            let term = reply_rx.await.map_err(|_| CallError::Terminated)?;
            Ok(term)
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
