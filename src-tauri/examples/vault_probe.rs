//! 隔离 vault 读写验证（A1 专用，本地临时工具，不提交）
//! 只使用传入的 key 文件在独立目录创建/重开 vault，不触碰 Keychain 与生产数据。
use tauri_plugin_stronghold::stronghold::Stronghold;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 3 {
        eprintln!("usage: vault_probe <out_dir> <key_file>");
        std::process::exit(2);
    }
    let dir = std::path::PathBuf::from(&a[1]);
    std::fs::create_dir_all(&dir).expect("mkdir");
    let raw = std::fs::read_to_string(&a[2]).expect("read key file");
    let hex = raw.trim();
    let pwd: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex decode"))
        .collect();
    println!("STEP1 password_len={}", pwd.len());

    let path = dir.join("vault.stronghold");
    let sh = Stronghold::new(&path, pwd.clone()).expect("Stronghold::new");
    sh.create_client(b"main".to_vec()).expect("create_client");
    println!("STEP2 client_created");

    {
        let c = sh.get_client(b"main".to_vec()).expect("get_client");
        c.store()
            .insert(b"ns:__probe__:k".to_vec(), br#"{"ok":true}"#.to_vec(), None)
            .expect("insert");
    }
    sh.save().expect("save");
    println!("STEP3 saved exists={}", path.exists());
    drop(sh);

    let sh2 = Stronghold::new(&path, pwd).expect("reopen");
    sh2.load_client(b"main".to_vec()).expect("load_client");
    let c2 = sh2.get_client(b"main".to_vec()).expect("get_client2");
    let v = c2
        .store()
        .get(&b"ns:__probe__:k".to_vec())
        .expect("get")
        .expect("record missing");
    println!("STEP4 READBACK_OK {}", String::from_utf8_lossy(&v));
    println!("RESULT=PASS");
}
