use nu_plugin::{MsgPackSerializer, serve_plugin};
use nu_plugin_df::DfPlugin;

fn main() {
    env_logger::init();
    match DfPlugin::new() {
        Ok(plugin) => serve_plugin(&plugin, MsgPackSerializer {}),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }
}
