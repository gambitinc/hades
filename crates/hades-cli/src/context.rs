//! Build-context packaging: tar.gz the context directory so the daemon (and
//! Docker's build endpoint) get a self-contained artifact. No shared-
//! filesystem assumptions — the same wire shape will work multi-machine.

use std::path::Path;

use flate2::write::GzEncoder;
use flate2::Compression;
use hades_core::HadesError;

const SKIP_DIRS: &[&str] = &[".git", "target", "node_modules", ".hades", "__pycache__"];

pub fn pack(context_dir: &Path) -> Result<Vec<u8>, HadesError> {
    if !context_dir.is_dir() {
        return Err(HadesError::InvalidSpec(format!(
            "build context {} is not a directory",
            context_dir.display()
        )));
    }
    let buf = Vec::new();
    let enc = GzEncoder::new(buf, Compression::default());
    let mut tar = tar::Builder::new(enc);
    tar.follow_symlinks(false);
    add_dir(&mut tar, context_dir, Path::new(""))?;
    let enc = tar
        .into_inner()
        .map_err(|e| HadesError::Other(format!("tar failed: {e}")))?;
    enc.finish()
        .map_err(|e| HadesError::Other(format!("gzip failed: {e}")))
}

fn add_dir(
    tar: &mut tar::Builder<GzEncoder<Vec<u8>>>,
    dir: &Path,
    rel: &Path,
) -> Result<(), HadesError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let path = entry.path();
        let rel_path = rel.join(&name);
        if path.is_dir() {
            if SKIP_DIRS.contains(&name_str.as_ref()) {
                continue;
            }
            add_dir(tar, &path, &rel_path)?;
        } else if path.is_file() {
            tar.append_path_with_name(&path, &rel_path)
                .map_err(|e| HadesError::Other(format!("tar {}: {e}", path.display())))?;
        }
    }
    Ok(())
}
