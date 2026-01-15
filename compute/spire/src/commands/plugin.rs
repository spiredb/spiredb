use crate::cli::PluginCmd;
use spire_common::SpireError;
use spire_proto::spiredb::cluster::plugin_service_client::PluginServiceClient;
use spire_proto::spiredb::cluster::{
    Empty, InstallPluginRequest, UninstallPluginRequest, install_plugin_request,
};
use std::path::Path;
use std::process::Command;
use tonic::transport::Channel;

pub async fn handle_plugin_command(cmd: PluginCmd, pd_addr: String) -> Result<(), SpireError> {
    match cmd {
        PluginCmd::List => {
            let mut client = connect(pd_addr).await?;
            let response = client.list_plugins(Empty {}).await?;
            let plugins = response.into_inner().plugins;
            println!("Installed Plugins:");
            for plugin in plugins {
                println!(
                    "  - {} (v{}) [Type: {:?}, State: {:?}]",
                    plugin.name, plugin.version, plugin.r#type, plugin.state
                );
            }
        }
        PluginCmd::Install { source } => {
            // Check if source is a local file
            let request = if Path::new(&source).exists() {
                let tarball = std::fs::read(&source).map_err(SpireError::Io)?;
                InstallPluginRequest {
                    source: Some(install_plugin_request::Source::Tarball(tarball)),
                }
            } else if source.starts_with("git://") || source.starts_with("https://github.com") {
                InstallPluginRequest {
                    source: Some(install_plugin_request::Source::GithubRepo(source)),
                }
            } else {
                InstallPluginRequest {
                    source: Some(install_plugin_request::Source::HexPackage(source)),
                }
            };

            let mut client = connect(pd_addr).await?;
            let response = client.install_plugin(request).await?;
            let plugin = response.into_inner();
            println!("Installed plugin: {} v{}", plugin.name, plugin.version);
        }
        PluginCmd::Uninstall { name } => {
            let mut client = connect(pd_addr).await?;
            client
                .uninstall_plugin(UninstallPluginRequest { name: name.clone() })
                .await?;
            println!("Uninstalled plugin: {}", name);
        }
        PluginCmd::New { name } => {
            scaffold_new_plugin(&name)?;
            println!("Created new plugin project: {}", name);
        }
        PluginCmd::Build => {
            build_plugin()?;
            println!("Plugin built successfully.");
        }
    }

    Ok(())
}

async fn connect(addr: String) -> Result<PluginServiceClient<Channel>, SpireError> {
    let endpoint = Channel::from_shared(addr.clone())
        .map_err(|e| SpireError::Internal(format!("Invalid URI: {}", e)))?;
    let channel = endpoint.connect().await.map_err(|e| {
        SpireError::Grpc(tonic::Status::unavailable(format!(
            "Failed to connect to {}: {}",
            addr, e
        )))
    })?;

    Ok(PluginServiceClient::new(channel))
}

fn scaffold_new_plugin(name: &str) -> Result<(), SpireError> {
    if Path::new(name).exists() {
        return Err(SpireError::Internal(format!(
            "Directory {} already exists",
            name
        )));
    }

    // Create directory structure
    std::fs::create_dir(name)?;
    std::fs::create_dir(format!("{}/lib", name))?;
    std::fs::create_dir(format!("{}/native", name))?;
    std::fs::create_dir(format!("{}/native/{}", name, name))?;

    // Create mix.exs
    let mix_exs = format!(
        r#"defmodule {}.MixProject do
  use Mix.Project

  def project do
    [
      app: :{},
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {{:rustler, "~> 0.29.1"}}
    ]
  end
end
"#,
        title_case(name),
        name
    );
    std::fs::write(format!("{}/mix.exs", name), mix_exs)?;

    Ok(())
}

fn build_plugin() -> Result<(), SpireError> {
    // Run mix compile
    let status = Command::new("mix").arg("compile").status()?;

    if !status.success() {
        return Err(SpireError::Internal("mix compile failed".to_string()));
    }
    // TODO: Implement packaging logic (tarball creation)
    Ok(())
}

fn title_case(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}
