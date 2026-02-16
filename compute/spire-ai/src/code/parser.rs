//! Tree-sitter based code parser.
//!
//! Extracts symbols (functions, structs, classes) from source files.

use crate::code::symbols::{CodeChunk, SymbolKind};

/// Detect language from file extension.
pub fn detect_language(path: &str) -> String {
    let ext = path
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_lowercase();

    match ext.as_str() {
        "rs" => "rust".to_string(),
        "py" => "python".to_string(),
        "js" | "jsx" => "javascript".to_string(),
        "ts" | "tsx" => "typescript".to_string(),
        "go" => "go".to_string(),
        other => other.to_string(),
    }
}

/// Parse a file into code chunks.
///
/// Falls back to line-based chunking for unsupported languages.
pub fn parse_file(path: &str, content: &str, language: &str) -> Vec<CodeChunk> {
    // Try tree-sitter parsing first
    match parse_with_treesitter(path, content, language) {
        Some(chunks) if !chunks.is_empty() => chunks,
        _ => fallback_chunking(path, content, language),
    }
}

fn parse_with_treesitter(path: &str, content: &str, language: &str) -> Option<Vec<CodeChunk>> {
    let ts_language = match language {
        "rust" => tree_sitter_rust::LANGUAGE,
        "python" => tree_sitter_python::LANGUAGE,
        "javascript" => tree_sitter_javascript::LANGUAGE,
        "typescript" => tree_sitter_typescript::LANGUAGE_TYPESCRIPT,
        "go" => tree_sitter_go::LANGUAGE,
        _ => return None,
    };

    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&ts_language.into())
        .ok()?;

    let tree = parser.parse(content, None)?;
    let root = tree.root_node();

    let mut chunks = Vec::new();
    extract_symbols(root, content, path, language, None, &mut chunks);

    Some(chunks)
}

fn extract_symbols(
    node: tree_sitter::Node,
    source: &str,
    file: &str,
    language: &str,
    parent: Option<&str>,
    chunks: &mut Vec<CodeChunk>,
) {
    let kind = classify_node(node.kind(), language);

    if let Some(symbol_kind) = kind {
        let code = &source[node.byte_range()];
        let name = extract_name(node, source);
        let docs = extract_docs(node, source);
        let signature = extract_signature(node, source, language);
        let start_line = node.start_position().row + 1;
        let end_line = node.end_position().row + 1;

        let id = format!(
            "{}:{}:{}",
            file,
            start_line,
            name.as_deref().unwrap_or("block")
        );

        chunks.push(CodeChunk {
            id,
            code: code.to_string(),
            file: file.to_string(),
            language: language.to_string(),
            kind: symbol_kind,
            name: name.clone(),
            start_line,
            end_line,
            parent: parent.map(|s| s.to_string()),
            docs,
            signature,
        });

        // Recurse into children with this as parent
        let parent_name = name.as_deref().or(parent);
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            extract_symbols(child, source, file, language, parent_name, chunks);
        }
    } else {
        // Not a symbol — recurse into children
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            extract_symbols(child, source, file, language, parent, chunks);
        }
    }
}

fn classify_node(node_kind: &str, _language: &str) -> Option<SymbolKind> {
    match node_kind {
        "function_item" | "function_definition" | "function_declaration" | "func_literal" => {
            Some(SymbolKind::Function)
        }
        "method_definition" | "method_declaration" => Some(SymbolKind::Method),
        "impl_item" | "class_definition" | "class_declaration" => Some(SymbolKind::Class),
        "struct_item" | "struct_definition" => Some(SymbolKind::Struct),
        "trait_item" => Some(SymbolKind::Trait),
        "interface_declaration" => Some(SymbolKind::Interface),
        "enum_item" | "enum_definition" => Some(SymbolKind::Enum),
        "mod_item" | "module" => Some(SymbolKind::Module),
        "const_item" | "static_item" => Some(SymbolKind::Constant),
        _ => None,
    }
}

fn extract_name(node: tree_sitter::Node, source: &str) -> Option<String> {
    // Look for a "name" field on the node
    if let Some(name_node) = node.child_by_field_name("name") {
        return Some(source[name_node.byte_range()].to_string());
    }
    None
}

fn extract_docs(node: tree_sitter::Node, source: &str) -> Option<String> {
    // Look for preceding comment nodes
    if let Some(prev) = node.prev_sibling() {
        let kind = prev.kind();
        if kind.contains("comment") || kind == "doc_comment" || kind == "line_comment" {
            return Some(source[prev.byte_range()].trim().to_string());
        }
    }
    None
}

fn extract_signature(node: tree_sitter::Node, source: &str, _language: &str) -> Option<String> {
    // For functions, extract just the signature (up to the body)
    if let Some(body) = node.child_by_field_name("body") {
        let sig_end = body.start_byte();
        let sig = &source[node.start_byte()..sig_end];
        return Some(sig.trim().to_string());
    }
    None
}

/// Fallback: split file into fixed-size line-based chunks.
fn fallback_chunking(path: &str, content: &str, language: &str) -> Vec<CodeChunk> {
    let lines: Vec<&str> = content.lines().collect();
    let chunk_size = 50; // lines per chunk
    let mut chunks = Vec::new();

    for (idx, window) in lines.chunks(chunk_size).enumerate() {
        let start_line = idx * chunk_size + 1;
        let end_line = start_line + window.len().saturating_sub(1);
        let code = window.join("\n");

        if code.trim().is_empty() {
            continue;
        }

        chunks.push(CodeChunk {
            id: format!("{path}:block-{idx}"),
            code,
            file: path.to_string(),
            language: language.to_string(),
            kind: SymbolKind::Block,
            name: None,
            start_line,
            end_line,
            parent: None,
            docs: None,
            signature: None,
        });
    }

    chunks
}
