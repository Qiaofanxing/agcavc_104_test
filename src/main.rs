//! AGC/AVC IEC 60870-5-104 双端人工测试台。
//!
//! 程序固定从 config/agcavc104test.toml 启动采集子站和调度主站，
//! 所有协议操作都在七页浅色 TUI 中手工触发或自动应答。

mod config;
mod model;
mod protocol;
mod runtime;
mod ui;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("agcavc104test 启动失败: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    validate_argument_count(std::env::args_os().count())?;
    let config = config::load()?;
    let runtime = runtime::start(config.clone());
    let commands = runtime.commands.clone();
    let result = ui::run(&config, commands.clone(), runtime.snapshots.clone()).await;
    let _ = commands.send(model::RuntimeCommand::Quit).await;
    runtime.wait().await;
    result
}

fn validate_argument_count(count: usize) -> Result<(), String> {
    if count == 1 {
        Ok(())
    } else {
        Err("本程序不接受命令行参数；请只修改 config/agcavc104test.toml 后直接启动".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_accepts_only_the_program_name() {
        assert!(validate_argument_count(1).is_ok());
        assert!(validate_argument_count(2).is_err());
        assert!(validate_argument_count(0).is_err());
    }
}
