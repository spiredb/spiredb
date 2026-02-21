use std::path::Path;

pub fn detect_project_type(project_dir: &str) -> String {
    let dir = Path::new(project_dir);
    let checks: &[(&str, &str)] = &[
        ("Cargo.toml", "Rust"),
        ("package.json", "JavaScript/TypeScript"),
        ("tsconfig.json", "TypeScript"),
        ("go.mod", "Go"),
        ("pyproject.toml", "Python"),
        ("setup.py", "Python"),
        ("requirements.txt", "Python"),
        ("Gemfile", "Ruby"),
        ("pom.xml", "Java (Maven)"),
        ("build.gradle", "Java/Kotlin (Gradle)"),
        ("mix.exs", "Elixir"),
        ("CMakeLists.txt", "C/C++ (CMake)"),
        ("Makefile", "C/C++ (Make)"),
        ("composer.json", "PHP"),
        ("pubspec.yaml", "Dart/Flutter"),
        ("Package.swift", "Swift"),
    ];
    let mut types = Vec::new();
    for (file, lang) in checks {
        if dir.join(file).exists() {
            types.push(*lang);
        }
    }
    if types.is_empty() {
        "unknown".to_string()
    } else {
        types.join(" + ")
    }
}

pub fn build_system_prompt(project_type: &str, project_dir: &str) -> String {
    format!(
        "You are a coding assistant working on a {project_type} project at {project_dir}.\n\
         \n\
         TOOLS:\n\
         \n\
         Exploration:\n\
         - glob: Find files by pattern (e.g. '**/*.rs', 'src/**/*.ts').\n\
         - grep: Search file contents by regex. Optional 'glob' param to filter file types.\n\
         - list_files: List a single directory's contents.\n\
         - search_code: Semantic code search (meaning-based, not keyword).\n\
         - find_symbol: Find functions, structs, classes by name.\n\
         \n\
         File Operations:\n\
         - read_file: Read a file with line numbers. Always read before editing.\n\
         - edit_file: Replace exact text span (old_string -> new_string).\n\
         - write_file: Create new files or full overwrite. Prefer edit_file for existing files.\n\
         \n\
         Execution:\n\
         - bash: Run shell commands (build, test, install, git, etc). Requires confirmation.\n\
         \n\
         Memory:\n\
         - remember: Store notes for later.\n\
         - recall: Search stored memories.\n\
         \n\
         WORKFLOW:\n\
         1. Orient: glob to find files, grep for patterns, find_symbol for definitions.\n\
         2. Read: read_file before editing to get exact content.\n\
         3. Change: edit_file for targeted edits, write_file for new files.\n\
         4. Verify: bash to run tests/build, read_file to confirm changes.\n\
         \n\
         RULES:\n\
         - Paths are relative to the project root.\n\
         - edit_file: old_string must be a unique, exact match from current file content.\n\
         - Never guess at file contents — always read_file first.\n\
         - Be precise. Don't over-engineer or add unrelated changes.\n\
         - Explain what you changed and why."
    )
}
