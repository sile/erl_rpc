erl_rpc
=======

[![erl_rpc](https://img.shields.io/crates/v/erl_rpc.svg)](https://crates.io/crates/erl_rpc)
[![Documentation](https://docs.rs/erl_rpc/badge.svg)](https://docs.rs/erl_rpc)
[![Actions Status](https://github.com/sile/erl_rpc/workflows/CI/badge.svg)](https://github.com/sile/erl_rpc/actions)
[![Coverage Status](https://coveralls.io/repos/github/sile/erl_rpc/badge.svg?branch=main)](https://coveralls.io/github/sile/erl_rpc?branch=main)
![License](https://img.shields.io/crates/l/erl_rpc)

Erlang RPC Client for Rust.

Examples
--------

```rust
smol::block_on(async {
    // Connect to an Erlang node.
    let erlang_node = "foo@localhost";
    let cookie = "cookie-value";
    let client = erl_rpc::RpcClient::connect(erlang_node, cookie).await?;
    let mut handle = client.handle();

    // Run the RPC client as a background task.
    smol::spawn(async {
        if let Err(e) = client.run().await {
            eprintln!("RpcClient Error: {}", e);
        }
    }).detach();

    // Execute an RPC: `erlang:processes/0`
    let result = handle
        .call("erlang".into(), "processes".into(), erl_dist::term::List::nil())
        .await?;
    println!("{}", result);
    Ok(())
})
```
