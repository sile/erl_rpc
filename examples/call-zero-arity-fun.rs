use erl_dist::term::List;

fn main() -> noargs::Result<()> {
    let mut args = noargs::raw_args();
    args.metadata_mut().app_name = env!("CARGO_PKG_NAME");
    args.metadata_mut().app_description = env!("CARGO_PKG_DESCRIPTION");

    if noargs::VERSION_FLAG.take(&mut args).is_present() {
        println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    noargs::HELP_FLAG.take_help(&mut args);

    let cookie: Option<String> = noargs::opt("cookie")
        .doc("Erlang cookie")
        .take(&mut args)
        .present_and_then(|o| o.value().parse())?;
    let port: Option<u16> = noargs::opt("port")
        .short('p')
        .ty("PORT")
        .doc("Port number (for epmdless nodes)")
        .take(&mut args)
        .present_and_then(|o| o.value().parse())?;

    let node_name: String = noargs::arg("<NODE_NAME>")
        .doc("Target Erlang node name")
        .example("foo@localhost")
        .take(&mut args)
        .then(|a| a.value().parse())?;
    let module: String = noargs::arg("<MODULE>")
        .doc("Module name")
        .example("erlang")
        .take(&mut args)
        .then(|a| a.value().parse())?;
    let function: String = noargs::arg("<FUNCTION>")
        .doc("Function name")
        .example("node")
        .take(&mut args)
        .then(|a| a.value().parse())?;

    if let Some(help) = args.finish()? {
        print!("{help}");
        return Ok(());
    }

    let cookie = if let Some(cookie) = cookie {
        cookie
    } else if let Some(dir) = dirs::home_dir().filter(|dir| dir.join(".erlang.cookie").exists()) {
        std::fs::read_to_string(dir.join(".erlang.cookie"))?
    } else {
        return Err("Could not find the cookie file $HOME/.erlang.cookie. Please specify `--cookie` option instead.".into());
    };

    smol::block_on(async {
        let client = if let Some(port) = port {
            erl_rpc::RpcClient::connect_with_port(&node_name, port, &cookie).await?
        } else {
            erl_rpc::RpcClient::connect(&node_name, &cookie).await?
        };
        let mut handle = client.handle();
        smol::spawn(async {
            if let Err(e) = client.run().await {
                eprintln!("RpcClient Error: {}", e);
            }
        })
        .detach();

        let result = handle
            .call(module.into(), function.into(), List::nil())
            .await?;
        println!("{}", result);

        Ok(())
    })
}
