// SPDX-License-Identifier: AGPL-3.0-only

use std::process::Command;

fn main() {
  // Retrieve the current commit hash
  let commit_hash = Command::new("git")
    .args(["rev-parse", "--short", "HEAD"])
    .output()
    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
    .unwrap_or_else(|_| "unknown".to_string());

  // Retrieve the current branch name
  let branch_name = Command::new("git")
    .args(["rev-parse", "--abbrev-ref", "HEAD"])
    .output()
    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
    .unwrap_or_else(|_| "unknown".to_string());

  println!("cargo:rustc-env=GIT_COMMIT_HASH={}", commit_hash);
  println!("cargo:rustc-env=GIT_BRANCH_NAME={}", branch_name);

  // Re-run build script if .git/HEAD changes (indicating a new commit or branch change)
  println!("cargo:rerun-if-changed=.git/HEAD");
}
