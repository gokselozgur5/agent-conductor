//! Private Linux check helper; never used to launch full-capability workers.
fn main() {
    #[cfg(target_os = "linux")]
    {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let result = if args.len() == 1 && args[0].len() <= 64 * 1024 {
            serde_json::from_str::<conductor_runtime::LinuxCheckRequest>(&args[0])
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
                .and_then(|request| conductor_runtime::exec_linux_check(&request))
        } else {
            Err("invalid Linux check request".into())
        };
        if result.is_err() {
            eprintln!("Linux check isolation could not be established");
            std::process::exit(1);
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("Linux check isolation is unavailable on this host");
        std::process::exit(1);
    }
}
