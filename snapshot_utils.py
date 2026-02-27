#!/usr/bin/env python3
"""
Utility functions for creating repository snapshots.
Used by init.py and submit.py.
"""
import os
import zipfile
from pathlib import Path


def create_repository_snapshot_zip(source_dir, zip_file_path):
    """Create a zip file of the repository, excluding system files and .git/.claude directories."""
    try:
        if os.path.exists(zip_file_path):
            os.remove(zip_file_path)

        # Files and directories to exclude
        exclude_patterns = {'.git', '.claude', '.DS_Store', '__pycache__', '.vscode', '.idea',
                          'node_modules', '.pytest_cache', '.mypy_cache', '*.pyc', '*.pyo'}

        def should_exclude(file_path):
            """Check if file should be excluded from snapshot."""
            path_parts = Path(file_path).parts
            name = os.path.basename(file_path)

            # Check if any part of the path matches exclude patterns
            for part in path_parts:
                if part in exclude_patterns or part.startswith('.'):
                    # Allow some common dotfiles but exclude system ones
                    if part not in {'.gitignore', '.env.example', '.dockerignore'}:
                        return True

            # Check filename patterns
            if name.endswith(('.pyc', '.pyo', '.DS_Store')):
                return True

            return False

        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            source_path = Path(source_dir)

            for file_path in source_path.rglob('*'):
                if file_path.is_file():
                    relative_path = file_path.relative_to(source_path)

                    if not should_exclude(str(relative_path)):
                        zipf.write(file_path, relative_path)

        return True

    except Exception as e:
        print(f"Error creating repository snapshot zip: {e}")
        return False


def create_git_diff_patch(repo_dir, patch_file_path, base_commit):
    """Generate git diff from base commit to current state."""
    try:
        import subprocess
        import os

        original_cwd = os.getcwd()
        # Make patch path absolute before chdir
        patch_file_path = os.path.abspath(patch_file_path)
        os.chdir(repo_dir)

        if not base_commit:
            os.chdir(original_cwd)
            return False

        # Add untracked files with intent-to-add so they show in diff
        excluded_patterns = ['.claude/', '__pycache__/', 'node_modules/', '.mypy_cache/',
                           '.pytest_cache/', '.DS_Store', '.vscode/', '.idea/']

        untracked_result = subprocess.run(
            ['git', 'ls-files', '--others', '--exclude-standard'],
            capture_output=True,
            text=True,
            timeout=30
        )

        if untracked_result.returncode == 0 and untracked_result.stdout.strip():
            untracked_files = []
            for file in untracked_result.stdout.strip().split('\n'):
                file = file.strip()
                if file and not any(pattern in file for pattern in excluded_patterns):
                    untracked_files.append(file)

            for file in untracked_files:
                subprocess.run(['git', 'add', '-N', file], capture_output=True, timeout=5)

        # Generate git diff from base commit
        result = subprocess.run(
            ['git', 'diff', base_commit, '--', '.',
             ':!.claude', ':!**/.mypy_cache', ':!**/__pycache__', ':!**/.pytest_cache',
             ':!**/.DS_Store', ':!**/node_modules', ':!**/.vscode', ':!**/.idea'],
            capture_output=True,
            text=True,
            timeout=30
        )

        with open(patch_file_path, 'w', encoding='utf-8') as f:
            f.write(result.stdout)

        os.chdir(original_cwd)
        return result.returncode == 0

    except Exception as e:
        print(f"Error creating git diff patch: {e}")
        if 'original_cwd' in locals():
            os.chdir(original_cwd)
        return False


def get_base_commit_from_log(log_file):
    """Extract base commit from session_start event in log."""
    import json
    import os

    try:
        if not os.path.exists(log_file):
            return None

        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        event = json.loads(line)
                        if event.get('type') == 'session_start':
                            git_metadata = event.get('git_metadata', {})
                            return git_metadata.get('base_commit')
                    except json.JSONDecodeError:
                        continue
        return None
    except Exception:
        return None


def get_base_commit_for_model(logs_dir, model_lane):
    """Get base commit for a specific model from its session logs."""
    from pathlib import Path

    model_logs_dir = Path(logs_dir) / model_lane
    if not model_logs_dir.exists():
        return None

    # Find session log files (not raw)
    session_files = [f for f in model_logs_dir.glob("session_*.jsonl") if "_raw.jsonl" not in f.name]

    for session_file in session_files:
        base_commit = get_base_commit_from_log(str(session_file))
        if base_commit:
            return base_commit

    return None
