#!/usr/bin/env python3
"""
Backfill Session Summary Script
Generates session_summary from existing processed log when it's missing.
Does NOT require raw transcript - works from processed log only.
"""
import json
import sys
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

def read_processed_log(log_file_path):
    """Read processed log and extract all events."""
    if not os.path.exists(log_file_path):
        return None
    
    events = []
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    events.append(event)
                except json.JSONDecodeError:
                    continue
        return events
    except Exception as e:
        print(f"[ERROR] Reading processed log: {e}")
        return None

def has_session_summary(events):
    """Check if session_summary already exists."""
    for event in events:
        if event.get('type') == 'session_summary' or event.get('event_type') == 'session_summary':
            return True
    return False

def aggregate_token_usage(messages):
    """Aggregate token usage from assistant messages."""
    total_usage = {
        'total_input_tokens': 0,
        'total_output_tokens': 0,
        'total_cache_creation_tokens': 0,
        'total_cache_read_tokens': 0,
        'total_ephemeral_5m_tokens': 0,
        'total_ephemeral_1h_tokens': 0,
        'service_tier': None
    }
    
    for msg in messages:
        if msg.get('type') == 'assistant':
            message = msg.get('message', {})
            usage = message.get('usage', {})
            
            if usage:
                total_usage['total_input_tokens'] += usage.get('input_tokens', 0)
                total_usage['total_output_tokens'] += usage.get('output_tokens', 0)
                total_usage['total_cache_creation_tokens'] += usage.get('cache_creation_input_tokens', 0)
                total_usage['total_cache_read_tokens'] += usage.get('cache_read_input_tokens', 0)
                
                cache_creation = usage.get('cache_creation', {})
                total_usage['total_ephemeral_5m_tokens'] += cache_creation.get('ephemeral_5m_input_tokens', 0)
                total_usage['total_ephemeral_1h_tokens'] += cache_creation.get('ephemeral_1h_input_tokens', 0)
                
                if usage.get('service_tier'):
                    total_usage['service_tier'] = usage.get('service_tier')
    
    total_usage['total_actual_input_tokens'] = (
        total_usage['total_input_tokens'] + 
        total_usage['total_cache_creation_tokens'] + 
        total_usage['total_cache_read_tokens']
    )
    
    return total_usage

def categorize_user_messages(messages):
    """Categorize user messages into prompts, tool results, system."""
    user_prompts = 0
    tool_results = 0
    system_messages = 0
    
    for msg in messages:
        if msg.get('type') == 'user':
            message_content = msg.get('message', {}).get('content', '')
            
            if msg.get('isMeta'):
                system_messages += 1
            elif isinstance(message_content, list):
                has_tool_result = any(
                    isinstance(item, dict) and item.get('type') == 'tool_result'
                    for item in message_content
                )
                if has_tool_result:
                    tool_results += 1
                else:
                    user_prompts += 1
            elif isinstance(message_content, str) and (
                '<command-name>' in message_content or 
                '<local-command-stdout>' in message_content
            ):
                system_messages += 1
            elif isinstance(message_content, str):
                user_prompts += 1
            else:
                user_prompts += 1
    
    return {
        'user_prompts': user_prompts,
        'tool_results': tool_results,
        'system_messages': system_messages,
        'total_user_events': user_prompts + tool_results + system_messages
    }

def analyze_tool_calls(messages):
    """Extract tool call metrics from messages."""
    tool_calls = defaultdict(int)
    tool_results = defaultdict(int)
    
    for msg in messages:
        message = msg.get('message', {})
        content = message.get('content', [])
        
        if not isinstance(content, list):
            continue
        
        for item in content:
            if not isinstance(item, dict):
                continue
            
            if item.get('type') == 'tool_use':
                tool_name = item.get('name', 'unknown')
                tool_calls[tool_name] += 1
            elif item.get('type') == 'tool_result':
                tool_results['total'] += 1
    
    return {
        'tool_calls_by_type': dict(tool_calls),
        'total_tool_calls': sum(tool_calls.values()),
        'total_tool_results': tool_results.get('total', 0)
    }

def analyze_thinking_usage(messages):
    """Analyze thinking mode usage from processed messages."""
    thinking_stats = {
        'thinking_enabled_turns': 0,
        'thinking_disabled_turns': 0,
        'assistant_with_thinking_blocks': 0,
        'thinking_levels': defaultdict(int)
    }
    
    for msg in messages:
        if msg.get('type') == 'user' and 'thinkingMetadata' in msg:
            metadata = msg['thinkingMetadata']
            if not metadata.get('disabled', True):
                thinking_stats['thinking_enabled_turns'] += 1
                level = metadata.get('level', 'none')
                thinking_stats['thinking_levels'][level] += 1
            else:
                thinking_stats['thinking_disabled_turns'] += 1
        
        elif msg.get('type') == 'assistant':
            message = msg.get('message', {})
            content = message.get('content', [])
            if isinstance(content, list):
                has_thinking = any(
                    isinstance(item, dict) and item.get('type') == 'thinking'
                    for item in content
                )
                if has_thinking:
                    thinking_stats['assistant_with_thinking_blocks'] += 1
    
    return {
        'thinking_enabled_turns': thinking_stats['thinking_enabled_turns'],
        'thinking_disabled_turns': thinking_stats['thinking_disabled_turns'],
        'assistant_with_thinking_blocks': thinking_stats['assistant_with_thinking_blocks'],
        'thinking_levels': dict(thinking_stats['thinking_levels'])
    }

def calculate_git_metrics(cwd, base_commit):
    """Calculate git metrics from diff."""
    try:
        original_cwd = os.getcwd()
        os.chdir(cwd)
        
        if not base_commit:
            os.chdir(original_cwd)
            return {}
        
        # Add untracked files
        excluded_patterns = ['.claude/', '__pycache__/', 'node_modules/', '.mypy_cache/', 
                           '.pytest_cache/', '.DS_Store', '.vscode/', '.idea/']
        
        untracked_result = subprocess.run(
            ['git', 'ls-files', '--others', '--exclude-standard'],
            capture_output=True, text=True, timeout=30
        )
        
        if untracked_result.returncode == 0 and untracked_result.stdout.strip():
            untracked_files = [
                f.strip() for f in untracked_result.stdout.strip().split('\n')
                if f.strip() and not any(pattern in f for pattern in excluded_patterns)
            ]
            
            for file in untracked_files:
                subprocess.run(['git', 'add', '-N', file], capture_output=True, timeout=5)
        
        # Calculate numstat
        result = subprocess.run(
            ['git', 'diff', '--numstat', base_commit, '--', '.', 
             ':!.claude', ':!**/.mypy_cache', ':!**/__pycache__', ':!**/.pytest_cache',
             ':!**/.DS_Store', ':!**/node_modules', ':!**/.vscode', ':!**/.idea'],
            capture_output=True, text=True, timeout=30
        )
        
        os.chdir(original_cwd)
        
        if result.returncode != 0:
            return {}
        
        lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
        files_changed = 0
        total_lines_changed = 0
        
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                if len(parts) >= 3:
                    try:
                        added = int(parts[0]) if parts[0] != '-' else 0
                        removed = int(parts[1]) if parts[1] != '-' else 0
                        files_changed += 1
                        total_lines_changed += added + removed
                    except ValueError:
                        continue
        
        return {
            "files_changed_count": files_changed,
            "lines_of_code_changed_count": total_lines_changed
        }
    except Exception as e:
        print(f"Warning: Could not calculate git metrics: {e}")
        if 'original_cwd' in locals():
            os.chdir(original_cwd)
        return {}

def backfill_session_summary(log_file_path):
    """Generate and append session_summary to processed log if missing."""
    
    # Read all events
    events = read_processed_log(log_file_path)
    if not events:
        print(f"[ERROR] Could not read log file: {log_file_path}")
        return False
    
    # Check if summary already exists
    if has_session_summary(events):
        print(f"[OK] Session summary already exists in {log_file_path}")
        return True
    
    print(f"[INFO] Backfilling session summary for {log_file_path}")
    
    # Separate messages from other events
    messages = [e for e in events if e.get('type') in ['assistant', 'user']]
    session_start = next((e for e in events if e.get('type') == 'session_start'), None)
    
    if not messages:
        print("[ERROR] No messages found in processed log")
        return False
    
    # Extract metadata from first message
    first_msg = messages[0] if messages else session_start or events[0]
    session_id = first_msg.get('session_id', 'unknown')
    cwd = first_msg.get('cwd', '')
    transcript_path = first_msg.get('transcript_path', '')
    
    # Calculate metrics
    usage_totals = aggregate_token_usage(messages)
    user_metrics = categorize_user_messages(messages)
    tool_metrics = analyze_tool_calls(messages)
    thinking_metrics = analyze_thinking_usage(messages)
    
    # Calculate duration from timestamps
    timestamps = []
    for msg in messages:
        ts = msg.get('timestamp')
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                timestamps.append(dt)
            except:
                continue
    
    total_duration = 0
    if len(timestamps) >= 2:
        duration = max(timestamps) - min(timestamps)
        total_duration = duration.total_seconds()
    
    # Count messages with proper categorization
    # Note: assistant_thinking is NOT counted as a separate message (it's part of assistant message)
    assistant_count = sum(1 for m in messages if m.get('type') == 'assistant')
    thinking_count = sum(1 for m in messages if m.get('type') == 'assistant_thinking')
    total_user_events = user_metrics['total_user_events']

    # Calculate actual total messages (excluding thinking blocks as they're not separate messages)
    actual_total_messages = assistant_count + total_user_events
    
    # Get git metrics if we have base_commit
    base_commit = None
    if session_start:
        git_metadata = session_start.get('git_metadata', {})
        base_commit = git_metadata.get('base_commit')
    
    git_metrics = calculate_git_metrics(cwd, base_commit) if base_commit and cwd else {}
    
    # Detect model lane for file references
    path_parts = Path(cwd).parts if cwd else []
    if 'model_a' in path_parts:
        model_lane = 'model_a'
    elif 'model_b' in path_parts:
        model_lane = 'model_b'
    else:
        model_lane = None
    
    # Use last message timestamp as session end time (more accurate than backfill time)
    session_end_timestamp = max(timestamps).isoformat() if timestamps else datetime.now(timezone.utc).isoformat()

    # Build session summary
    summary = {
        "type": "session_summary",
        "timestamp": session_end_timestamp,
        "session_id": session_id,
        "transcript_path": transcript_path,
        "cwd": cwd,
        "summary_data": {
            "total_duration_seconds": round(total_duration, 2),
            "total_messages": actual_total_messages,
            "assistant_messages": assistant_count,
            "user_prompts": user_metrics['user_prompts'],
            "user_metrics": user_metrics,
            "usage_totals": usage_totals,
            "tool_metrics": tool_metrics,
            "thinking_metrics": {
                **thinking_metrics,
                "assistant_thinking_blocks_captured": thinking_count
            },
            "git_metrics": git_metrics,
            "files": {
                "processed_log": Path(log_file_path).name,
                "raw_transcript": Path(log_file_path).stem.replace('session_', 'session_') + '_raw.jsonl',
                "git_diff": f"{model_lane}_diff.patch" if model_lane else None
            },
            "validation": {
                "complete": True,
                "unique_messages_processed": actual_total_messages,
                "thinking_blocks_extracted": thinking_count,
                "backfilled": True
            }
        }
    }
    
    # Copy metadata from first message if available
    if first_msg:
        for key in ['task_id', 'model_lane', 'experiment_root', 'model_name']:
            if key in first_msg:
                summary[key] = first_msg[key]
    
    # Append summary to log file
    try:
        with open(log_file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(summary) + '\n')
        
        print(f"[OK] Backfilled session summary")
        print(f"     Messages: {actual_total_messages} ({assistant_count} assistant, {user_metrics['user_prompts']} user prompts)")
        if thinking_count > 0:
            print(f"     Thinking blocks: {thinking_count} (tokens already included in assistant output)")
        print(f"     Tokens: {usage_totals['total_actual_input_tokens']:,} total input, {usage_totals['total_output_tokens']:,} output")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to append summary: {e}")
        return False

def process_directory(directory_path):
    """Process all session logs in a directory."""
    directory = Path(directory_path)
    
    if not directory.exists():
        print(f"[ERROR] Directory not found: {directory_path}")
        return
    
    # Find all session log files (not _raw)
    session_files = list(directory.glob("session_*.jsonl"))
    session_files = [f for f in session_files if not f.name.endswith('_raw.jsonl')]
    
    if not session_files:
        print(f"[INFO] No session files found in {directory_path}")
        return
    
    print(f"[INFO] Found {len(session_files)} session file(s) in {directory_path}")
    
    success_count = 0
    skip_count = 0
    fail_count = 0
    
    for session_file in session_files:
        print(f"\nProcessing: {session_file.name}")
        
        events = read_processed_log(session_file)
        if not events:
            fail_count += 1
            continue
        
        if has_session_summary(events):
            print(f"[OK] Already has summary, skipping")
            skip_count += 1
            continue
        
        if backfill_session_summary(session_file):
            success_count += 1
        else:
            fail_count += 1
    
    print("\n" + "=" * 70)
    print("BACKFILL SUMMARY")
    print("=" * 70)
    print(f"  Total files: {len(session_files)}")
    print(f"  Backfilled: {success_count}")
    print(f"  Already had summary: {skip_count}")
    print(f"  Failed: {fail_count}")

def main():
    if len(sys.argv) < 2:
        print("Backfill Session Summary Script")
        print("=" * 70)
        print("\nUsage:")
        print("  python backfill_session_summary.py <log_file_or_directory>")
        print("\nExamples:")
        print("  # Single file:")
        print("  python backfill_session_summary.py logs/model_a/session_abc123.jsonl")
        print("")
        print("  # Entire directory:")
        print("  python backfill_session_summary.py logs/model_a/")
        print("")
        print("  # All logs:")
        print("  python backfill_session_summary.py logs/")
        sys.exit(1)
    
    path = sys.argv[1]
    path_obj = Path(path)
    
    if path_obj.is_file():
        # Single file
        print(f"Backfilling single file: {path}")
        if backfill_session_summary(path):
            print("\n[OK] Backfill complete!")
        else:
            print("\n[ERROR] Backfill failed")
            sys.exit(1)
    
    elif path_obj.is_dir():
        # Directory - process recursively
        print(f"Backfilling directory: {path}")
        
        # Find all model_a and model_b subdirectories
        if (path_obj / 'model_a').exists() or (path_obj / 'model_b').exists():
            # This is an experiment root with model_a/model_b
            for model in ['model_a', 'model_b']:
                model_logs = path_obj / 'logs' / model
                if model_logs.exists():
                    print(f"\n{'=' * 70}")
                    print(f"Processing {model}")
                    print('=' * 70)
                    process_directory(model_logs)
        else:
            # This is a logs directory
            process_directory(path)
        
        print("\n[OK] Backfill complete!")
    
    else:
        print(f"[ERROR] Path not found: {path}")
        sys.exit(1)

if __name__ == "__main__":
    main()

