use clap::Parser;
use erl_dist::term::{Atom, List};

#[derive(Debug, Parser)]
struct Args {
    node_name: erl_dist::node::NodeName,

    #[clap(long)]
    cookie: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cookie = if let Some(cookie) = &args.cookie {
        cookie.clone()
    } else if let Some(dir) = dirs::home_dir().filter(|dir| dir.join(".erlang.cookie").exists()) {
        std::fs::read_to_string(dir.join(".erlang.cookie"))?
    } else {
        anyhow::bail!("Could not find the cookie file $HOME/.erlang.cookie. Please specify `-cookie` arg instead.");
    };

    smol::block_on(async {
        let client = erl_rpc::RpcClient::connect(&args.node_name.to_string(), &cookie).await?;
        let mut handle = client.handle();
        smol::spawn(client.run()).detach();

        let result = handle
            .call(
                "erlang".into(),
                "system_info".into(),
                List::from(vec![Atom::from("otp_release").into()]),
            )
            .await?;
        eprintln!("Result: {:?}", result);

        Ok(())
    })
}
