//! File system walker with .gitignore support.

use ignore::WalkBuilder;

use crate::error::Result;

/// Walk a directory respecting .gitignore, returning file paths.
pub fn walk_dir(path: &str) -> Result<Vec<String>> {
    let mut files = Vec::new();

    for entry in WalkBuilder::new(path)
        .hidden(true)
        .git_ignore(true)
        .build()
    {
        let entry = entry.map_err(|e| crate::error::Error::Other(e.to_string()))?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                let ext = ext.to_string_lossy();
                if is_code_file(&ext) {
                    files.push(path.to_string_lossy().to_string());
                }
            }
        }
    }

    Ok(files)
}

fn is_code_file(ext: &str) -> bool {
    matches!(
        ext,
        "rs" | "py"
            | "js"
            | "ts"
            | "tsx"
            | "jsx"
            | "go"
            | "java"
            | "c"
            | "cpp"
            | "h"
            | "hpp"
            | "rb"
            | "ex"
            | "exs"
            | "erl"
            | "zig"
            | "swift"
            | "kt"
            | "scala"
            | "ml"
            | "hs"
    )
}
