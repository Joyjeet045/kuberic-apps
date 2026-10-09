use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result, ensure};

use crate::batch::{FORMAT_VERSION, STORAGE_PROFILE};

const MAGIC: &[u8; 8] = b"KRCOPY01";
pub const COPY_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_COPY_BYTES: usize = 128 * 1024 * 1024;

pub(crate) fn pack(directory: &Path, lsn: i64) -> Result<Vec<u8>> {
    let mut paths = fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    ensure!(paths.len() <= 4096, "checkpoint contains too many files");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&lsn.to_le_bytes());
    field(&mut bytes, STORAGE_PROFILE.as_bytes())?;
    bytes.extend_from_slice(&u32::try_from(paths.len())?.to_le_bytes());
    for path in paths {
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(metadata.is_file(), "checkpoint contains a nonregular file");
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .context("invalid filename")?;
        ensure!(valid_name(name), "unsupported checkpoint filename: {name}");
        ensure!(
            metadata.len() <= u64::try_from(MAX_COPY_BYTES.saturating_sub(bytes.len()))?,
            "checkpoint exceeds 128 MiB"
        );
        field(&mut bytes, name.as_bytes())?;
        field(&mut bytes, &fs::read(&path)?)?;
        ensure!(
            bytes.len() <= MAX_COPY_BYTES - 4,
            "checkpoint exceeds 128 MiB"
        );
    }
    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
}

fn field(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    output.extend_from_slice(&u32::try_from(value.len())?.to_le_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn valid_name(name: &str) -> bool {
    if matches!(name, "CURRENT" | "IDENTITY") {
        return true;
    }
    for prefix in ["MANIFEST-", "OPTIONS-"] {
        if let Some(number) = name.strip_prefix(prefix) {
            return !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit());
        }
    }
    for suffix in [".sst", ".log", ".blob"] {
        if let Some(number) = name.strip_suffix(suffix) {
            return !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit());
        }
    }
    false
}

struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        ensure!(size <= self.remaining.len(), "truncated checkpoint");
        let (value, remaining) = self.remaining.split_at(size);
        self.remaining = remaining;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }

    fn field(&mut self) -> Result<&'a [u8]> {
        let size = usize::try_from(self.u32()?)?;
        self.take(size)
    }
}

pub(crate) fn unpack(bytes: &[u8], directory: &Path, expected_lsn: i64) -> Result<()> {
    ensure!(
        (4..=MAX_COPY_BYTES).contains(&bytes.len()),
        "invalid checkpoint length"
    );
    let (payload, checksum) = bytes.split_at(bytes.len() - 4);
    ensure!(
        crc32fast::hash(payload) == u32::from_le_bytes(checksum.try_into()?),
        "checkpoint checksum mismatch"
    );
    let mut reader = Reader { remaining: payload };
    ensure!(reader.take(8)? == MAGIC, "invalid checkpoint magic");
    ensure!(
        reader.u32()? == FORMAT_VERSION,
        "unsupported checkpoint format"
    );
    let lsn = i64::from_le_bytes(reader.take(8)?.try_into()?);
    ensure!(lsn == expected_lsn && lsn >= 0, "checkpoint LSN mismatch");
    ensure!(
        reader.field()? == STORAGE_PROFILE.as_bytes(),
        "incompatible checkpoint profile"
    );
    let count = reader.u32()?;
    ensure!(count > 0 && count <= 4096, "invalid checkpoint file count");
    let mut names = BTreeSet::new();
    let mut files = Vec::new();
    for _ in 0..count {
        let name = std::str::from_utf8(reader.field()?)?;
        ensure!(
            valid_name(name),
            "unsafe or unsupported checkpoint filename"
        );
        ensure!(names.insert(name), "duplicate checkpoint file");
        files.push((name, reader.field()?));
    }
    ensure!(reader.remaining.is_empty(), "trailing checkpoint bytes");
    ensure!(names.contains("CURRENT"), "checkpoint has no CURRENT file");
    fs::create_dir(directory)?;
    for (name, contents) in files {
        let mut file = File::options()
            .write(true)
            .create_new(true)
            .open(directory.join(name))?;
        file.write_all(contents)?;
        file.sync_all()?;
    }
    sync_directory(directory)?;
    Ok(())
}

pub(crate) fn sync_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reseal(bytes: &mut [u8]) {
        let size = bytes.len() - 4;
        let crc = crc32fast::hash(&bytes[..size]);
        bytes[size..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn untrusted_checkpoint_paths_profiles_lengths_and_checksums_fail_before_writes() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("CURRENT"), b"MANIFEST-000001\n").unwrap();
        let bytes = pack(&source, 7).unwrap();
        let destination = root.path().join("destination");
        let mut traversal = bytes.clone();
        let position = traversal
            .windows(7)
            .position(|window| window == b"CURRENT")
            .unwrap();
        traversal[position..position + 7].copy_from_slice(b"../evil");
        reseal(&mut traversal);
        assert!(unpack(&traversal, &destination, 7).is_err());
        let mut profile = bytes.clone();
        profile[24] ^= 1;
        reseal(&mut profile);
        assert!(unpack(&profile, &destination, 7).is_err());
        let mut corrupt = bytes.clone();
        corrupt[0] ^= 1;
        assert!(unpack(&corrupt, &destination, 7).is_err());
        assert!(unpack(&bytes[..bytes.len() - 1], &destination, 7).is_err());
        assert!(unpack(&bytes, &destination, 8).is_err());
        assert!(unpack(&vec![0; MAX_COPY_BYTES + 1], &destination, 7).is_err());
        assert!(!destination.exists());
        assert!(!root.path().join("evil").exists());
    }
}
