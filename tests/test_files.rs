use codebase_mcp::tools::{count_file_lines, read_file};
use serde_json::json;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[tokio::test]
async fn test_read_file_range_with_encoding() {
    let dir = tempdir().unwrap();

    let utf8_path = dir.path().join("utf8.txt");
    let mut f1 = File::create(&utf8_path).unwrap();
    writeln!(f1, "Line 1\nLine 2\nTiếng Việt\nLine 4\nLine 5").unwrap();

    let args1 = json!({
        "path": utf8_path.to_str().unwrap(),
        "start_line": 2,
        "end_line": 4
    });

    let res1 = read_file::execute(&args1).await.unwrap();
    let content1 = res1.get("content").unwrap().as_str().unwrap();
    assert_eq!(content1, "Line 2\nTiếng Việt\nLine 4");
    assert_eq!(res1.get("encoding").unwrap().as_str().unwrap(), "UTF-8");

    let args2 = json!({
        "path": utf8_path.to_str().unwrap(),
        "start_line": 4,
        "end_line": 100
    });

    let res2 = read_file::execute(&args2).await.unwrap();
    let content2 = res2.get("content").unwrap().as_str().unwrap();
    assert_eq!(content2, "Line 4\nLine 5\n");
    assert_eq!(res2.get("total_lines").unwrap().as_u64().unwrap(), 5);

    let win1252_path = dir.path().join("win1252.txt");
    let mut f2 = File::create(&win1252_path).unwrap();
    f2.write_all(b"\xC7a va\nline 2").unwrap();

    let args3 = json!({
        "path": win1252_path.to_str().unwrap(),
    });

    let res3 = read_file::execute(&args3).await.unwrap();
    let content3 = res3.get("content").unwrap().as_str().unwrap();
    assert_eq!(
        res3.get("encoding").unwrap().as_str().unwrap(),
        "windows-1252"
    );
    assert_eq!(content3, "Ça va\nline 2");
}

#[tokio::test]
async fn test_read_file_size_limit() {
    let dir = tempdir().unwrap();
    let big_path = dir.path().join("big.bin");

    {
        let f = File::create(&big_path).unwrap();
        f.set_len(12 * 1024 * 1024).unwrap();
    }

    let args = json!({
        "path": big_path.to_str().unwrap(),
    });

    let res = read_file::execute(&args).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("too large"));
}

#[tokio::test]
async fn test_count_file_lines_handles_empty_plain_utf16_and_binary_files() {
    let dir = tempdir().unwrap();

    let empty_path = dir.path().join("empty.txt");
    std::fs::write(&empty_path, "").unwrap();
    let empty_res = count_file_lines::execute(&json!({
        "path": empty_path.to_str().unwrap()
    }))
    .await
    .unwrap();
    assert_eq!(empty_res.get("line_count").unwrap().as_u64().unwrap(), 0);
    assert!(!empty_res.get("is_binary").unwrap().as_bool().unwrap());
    assert_eq!(
        empty_res.get("encoding").unwrap().as_str().unwrap(),
        "UTF-8"
    );

    let plain_path = dir.path().join("plain.txt");
    std::fs::write(&plain_path, "alpha\nbeta\ngamma").unwrap();
    let plain_res = count_file_lines::execute(&json!({
        "path": plain_path.to_str().unwrap()
    }))
    .await
    .unwrap();
    assert_eq!(plain_res.get("line_count").unwrap().as_u64().unwrap(), 3);
    assert!(
        !plain_res
            .get("ends_with_newline")
            .unwrap()
            .as_bool()
            .unwrap()
    );

    let utf16_path = dir.path().join("utf16.txt");
    let mut utf16_bytes = vec![0xFF, 0xFE];
    for unit in "first\nsecond\n".encode_utf16() {
        utf16_bytes.extend_from_slice(&unit.to_le_bytes());
    }
    std::fs::write(&utf16_path, utf16_bytes).unwrap();
    let utf16_res = count_file_lines::execute(&json!({
        "path": utf16_path.to_str().unwrap()
    }))
    .await
    .unwrap();
    assert_eq!(utf16_res.get("line_count").unwrap().as_u64().unwrap(), 2);
    assert!(!utf16_res.get("is_binary").unwrap().as_bool().unwrap());
    assert_eq!(
        utf16_res.get("encoding").unwrap().as_str().unwrap(),
        "UTF-16LE"
    );
    assert!(
        utf16_res
            .get("ends_with_newline")
            .unwrap()
            .as_bool()
            .unwrap()
    );

    let binary_path = dir.path().join("sample.bin");
    std::fs::write(&binary_path, [0x01u8, 0x00, 0x02, 0x03]).unwrap();
    let binary_res = count_file_lines::execute(&json!({
        "path": binary_path.to_str().unwrap()
    }))
    .await
    .unwrap();
    assert!(binary_res.get("is_binary").unwrap().as_bool().unwrap());
    assert_eq!(binary_res.get("line_count").unwrap().as_u64().unwrap(), 0);
    assert!(binary_res.get("encoding").unwrap().is_null());
}
