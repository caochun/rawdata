"""Git operations wrapper."""
from __future__ import annotations

import json
from pathlib import Path

import git


class GitError(Exception):
    pass


def get_repo(repo_root: Path) -> git.Repo:
    try:
        return git.Repo(repo_root)
    except git.InvalidGitRepositoryError:
        raise GitError(f"{repo_root} is not a git repository")


def diff_summary(repo_root: Path) -> str:
    """Summarize staged CSV changes as a short commit message prefix.

    Stages all *.csv files first so the diff reflects working-tree state.
    Returns a string like "data: opportunity(+2,-1), customer(+1,-0)"
    or "" if there is nothing to commit.
    """
    repo = get_repo(repo_root)
    csv_files = list(repo_root.glob("*.csv"))
    if csv_files:
        repo.index.add([str(p.relative_to(repo_root)) for p in csv_files])

    parts = []
    for d in repo.index.diff("HEAD", create_patch=True):
        path = d.a_path or d.b_path or ""
        if not path.endswith(".csv"):
            continue
        table = path[:-4]  # strip .csv
        raw = d.diff.decode("utf-8", errors="replace") if d.diff else ""
        lines = raw.splitlines()
        added   = sum(1 for l in lines if l.startswith("+") and not l.startswith("+++"))
        removed = sum(1 for l in lines if l.startswith("-") and not l.startswith("---"))
        parts.append(f"{table}(+{added},-{removed})")

    for f in repo.untracked_files:
        if f.endswith(".csv"):
            parts.append(f"{f[:-4]}(new)")

    return ("data: " + ", ".join(parts)) if parts else ""


def commit_changes(repo_root: Path, message: str) -> str:
    """Stage all csv changes and commit. Returns commit sha."""
    repo = get_repo(repo_root)
    repo.index.add([str(p.relative_to(repo_root)) for p in repo_root.glob("*.csv")])
    if not repo.index.diff("HEAD") and not repo.untracked_files:
        return ""
    commit = repo.index.commit(message)
    return commit.hexsha[:8]


def sync(repo_root: Path) -> dict:
    """
    Pull remote, attempt auto-merge.
    Returns { "status": "ok" | "conflicts" | "no_remote", "conflicts": [...] }
    """
    repo = get_repo(repo_root)

    if not repo.remotes:
        return {
            "status": "no_remote",
            "message": (
                "No remote configured. "
                "Push this repo to GitHub first: "
                "git remote add origin <url> && git push -u origin main"
            ),
            "conflicts": [],
        }

    origin = repo.remotes.origin
    origin.fetch()

    branch_name = repo.active_branch.name
    if branch_name not in [r.remote_head for r in origin.refs]:
        return {
            "status": "no_remote",
            "message": (
                f"Branch '{branch_name}' has not been pushed yet. "
                "Run: git push -u origin " + branch_name
            ),
            "conflicts": [],
        }

    remote_ref = origin.refs[branch_name]
    base = repo.merge_base(repo.head.commit, remote_ref.commit)
    if not base:
        raise GitError("No common ancestor found")

    conflicts = _detect_conflicts(repo, remote_ref)
    if conflicts:
        return {"status": "conflicts", "conflicts": conflicts}

    origin.pull(branch_name)
    return {"status": "ok", "conflicts": []}


def _detect_conflicts(repo: git.Repo, remote_ref) -> list[dict]:
    """
    Compare local HEAD vs remote HEAD at the CSV row level.
    Returns structured conflict list for agent consumption.
    """
    conflicts = []
    local_commit = repo.head.commit
    remote_commit = remote_ref.commit
    base_commits = repo.merge_base(local_commit, remote_commit)
    if not base_commits:
        return []
    base_commit = base_commits[0]

    def read_csv_from_commit(commit, filename) -> dict[str, dict]:
        try:
            blob = commit.tree[filename]
            import csv, io
            reader = csv.DictReader(io.StringIO(blob.data_stream.read().decode()))
            return {row["id"]: row for row in reader if "id" in row}
        except KeyError:
            return {}

    local_files = {b.path for b in local_commit.tree.blobs if b.path.endswith(".csv")}
    remote_files = {b.path for b in remote_commit.tree.blobs if b.path.endswith(".csv")}

    for csv_file in local_files | remote_files:
        table = csv_file.replace(".csv", "")
        base_rows = read_csv_from_commit(base_commit, csv_file)
        local_rows = read_csv_from_commit(local_commit, csv_file)
        remote_rows = read_csv_from_commit(remote_commit, csv_file)

        all_ids = set(base_rows) | set(local_rows) | set(remote_rows)
        for row_id in all_ids:
            base = base_rows.get(row_id, {})
            local = local_rows.get(row_id, {})
            remote = remote_rows.get(row_id, {})

            for field in set(base) | set(local) | set(remote):
                bv = base.get(field)
                lv = local.get(field)
                rv = remote.get(field)
                if lv != rv and lv != bv and rv != bv:
                    conflicts.append({
                        "id": f"{table}:{row_id}:{field}",
                        "table": table,
                        "row_id": row_id,
                        "field": field,
                        "base": bv,
                        "mine": lv,
                        "theirs": rv,
                    })

    return conflicts


def push(repo_root: Path) -> None:
    try:
        repo = get_repo(repo_root)
        repo.remotes.origin.push(repo.active_branch.name)
    except git.GitCommandError as e:
        msg = (e.stderr or str(e)).strip()
        raise GitError(f"Push failed: {msg}")
