#!/usr/bin/env python3
"""
Merge multiple session transcripts into one unified session.
Cross-platform support for Windows, macOS, and Linux.

Usage:
    python merge_sessions.py .
"""

import json
import sys
import os
import uuid
import shutil
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple


def parse_timestamp(ts: str) -> datetime:
    """Parse ISO timestamp string to datetime."""
    if not ts:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        # Handle various ISO formats
        ts = ts.replace('Z', '+00:00')
        return datetime.fromisoformat(ts)
    except:
        return datetime.min.replace(tzinfo=timezone.utc)


def find_session_files(logs_dir: Path) -> Tuple[List[Path], List[Path]]:
    """Find all processed and raw session files, sorted by earliest timestamp."""
    processed_files = []
    raw_files = []
    
    for f in logs_dir.glob("session_*.jsonl"):
        if "_raw.jsonl" in f.name:
            raw_files.append(f)
        else:
            processed_files.append(f)
    
    # Sort processed files by their session_start timestamp
    def get_start_time(filepath: Path) -> datetime:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    event = json.loads(line.strip())
                    if event.get('type') == 'session_start':
                        return parse_timestamp(event.get('timestamp', ''))
        except:
            pass
        return datetime.min.replace(tzinfo=timezone.utc)
    
    processed_files.sort(key=get_start_time)
    
    # Match raw files to processed files by session_id
    processed_ids = [f.stem.replace('session_', '') for f in processed_files]
    raw_files_sorted = []
    for sid in processed_ids:
        raw_path = logs_dir / f"session_{sid}_raw.jsonl"
        if raw_path.exists():
            raw_files_sorted.append(raw_path)
    
    return processed_files, raw_files_sorted


def read_session_events(filepath: Path) -> List[Dict]:
    """Read all events from a session file."""
    events = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    except Exception as e:
        print(f"Warning: Error reading {filepath}: {e}", file=sys.stderr)
    return events


def extract_session_data(events: List[Dict]) -> Dict:
    """Extract key data from session events."""
    data = {
        'session_start': None,
        'session_end': None,
        'session_summary': None,
        'messages': [],  # user, assistant, assistant_thinking
        'other_events': []
    }
    
    for event in events:
        event_type = event.get('type')
        
        if event_type == 'session_start':
            data['session_start'] = event
        elif event_type == 'session_end':
            data['session_end'] = event
        elif event_type == 'session_summary':
            data['session_summary'] = event
        elif event_type in ('user', 'assistant', 'assistant_thinking'):
            data['messages'].append(event)
        else:
            data['other_events'].append(event)
    
    return data


def aggregate_summaries(summaries: List[Dict]) -> Dict:
    """Aggregate multiple session summaries into one."""
    totals = {
        'total_duration_seconds': 0,
        'total_messages': 0,
        'assistant_messages': 0,
        'user_prompts': 0,
        'user_metrics': {
            'user_prompts': 0,
            'tool_results': 0,
            'system_messages': 0,
            'total_user_events': 0
        },
        'usage_totals': {
            'total_input_tokens': 0,
            'total_output_tokens': 0,
            'total_cache_creation_tokens': 0,
            'total_cache_read_tokens': 0,
            'total_ephemeral_5m_tokens': 0,
            'total_ephemeral_1h_tokens': 0,
            'service_tier': None,
            'total_actual_input_tokens': 0
        },
        'tool_metrics': {
            'tool_calls_by_type': defaultdict(int),
            'total_tool_calls': 0,
            'total_tool_results': 0
        },
        'thinking_metrics': {
            'thinking_enabled_turns': 0,
            'thinking_disabled_turns': 0,
            'assistant_with_thinking_blocks': 0,
            'thinking_levels': defaultdict(int),
            'assistant_thinking_blocks_captured': 0
        },
        'git_metrics': {
            'files_changed_count': 0,
            'lines_of_code_changed_count': 0
        }
    }
    
    for summary in summaries:
        sd = summary.get('summary_data', {})
        
        totals['total_duration_seconds'] += sd.get('total_duration_seconds', 0)
        totals['total_messages'] += sd.get('total_messages', 0)
        totals['assistant_messages'] += sd.get('assistant_messages', 0)
        totals['user_prompts'] += sd.get('user_prompts', 0)
        
        # User metrics
        um = sd.get('user_metrics', {})
        totals['user_metrics']['user_prompts'] += um.get('user_prompts', 0)
        totals['user_metrics']['tool_results'] += um.get('tool_results', 0)
        totals['user_metrics']['system_messages'] += um.get('system_messages', 0)
        totals['user_metrics']['total_user_events'] += um.get('total_user_events', 0)
        
        # Usage totals
        ut = sd.get('usage_totals', {})
        totals['usage_totals']['total_input_tokens'] += ut.get('total_input_tokens', 0)
        totals['usage_totals']['total_output_tokens'] += ut.get('total_output_tokens', 0)
        totals['usage_totals']['total_cache_creation_tokens'] += ut.get('total_cache_creation_tokens', 0)
        totals['usage_totals']['total_cache_read_tokens'] += ut.get('total_cache_read_tokens', 0)
        totals['usage_totals']['total_ephemeral_5m_tokens'] += ut.get('total_ephemeral_5m_tokens', 0)
        totals['usage_totals']['total_ephemeral_1h_tokens'] += ut.get('total_ephemeral_1h_tokens', 0)
        totals['usage_totals']['total_actual_input_tokens'] += ut.get('total_actual_input_tokens', 0)
        if ut.get('service_tier'):
            totals['usage_totals']['service_tier'] = ut['service_tier']
        
        # Tool metrics
        tm = sd.get('tool_metrics', {})
        for tool_name, count in tm.get('tool_calls_by_type', {}).items():
            totals['tool_metrics']['tool_calls_by_type'][tool_name] += count
        totals['tool_metrics']['total_tool_calls'] += tm.get('total_tool_calls', 0)
        totals['tool_metrics']['total_tool_results'] += tm.get('total_tool_results', 0)
        
        # Thinking metrics
        thm = sd.get('thinking_metrics', {})
        totals['thinking_metrics']['thinking_enabled_turns'] += thm.get('thinking_enabled_turns', 0)
        totals['thinking_metrics']['thinking_disabled_turns'] += thm.get('thinking_disabled_turns', 0)
        totals['thinking_metrics']['assistant_with_thinking_blocks'] += thm.get('assistant_with_thinking_blocks', 0)
        totals['thinking_metrics']['assistant_thinking_blocks_captured'] += thm.get('assistant_thinking_blocks_captured', 0)
        for level, count in thm.get('thinking_levels', {}).items():
            totals['thinking_metrics']['thinking_levels'][level] += count
        
        # Git metrics (use max - files can overlap between sessions)
        gm = sd.get('git_metrics', {})
        totals['git_metrics']['files_changed_count'] = max(
            totals['git_metrics']['files_changed_count'],
            gm.get('files_changed_count', 0)
        )
        totals['git_metrics']['lines_of_code_changed_count'] = max(
            totals['git_metrics']['lines_of_code_changed_count'],
            gm.get('lines_of_code_changed_count', 0)
        )
    
    # Convert defaultdicts to regular dicts
    totals['tool_metrics']['tool_calls_by_type'] = dict(totals['tool_metrics']['tool_calls_by_type'])
    totals['thinking_metrics']['thinking_levels'] = dict(totals['thinking_metrics']['thinking_levels'])
    
    return totals


def merge_sessions(logs_dir: Path) -> bool:
    """
    Merge all sessions in a directory into one unified session.
    
    Returns True if successful, False otherwise.
    """
    processed_files, raw_files = find_session_files(logs_dir)
    
    if len(processed_files) < 2:
        print(f"Found {len(processed_files)} session file(s). Need at least 2 sessions to merge.")
        if len(processed_files) == 1:
            print(f"Single session found: {processed_files[0].name}")
        return False
    
    print(f"Found {len(processed_files)} sessions to merge:")
    for f in processed_files:
        print(f"  - {f.name}")
    
    # Extract data from all sessions
    all_session_data = []
    for pf in processed_files:
        events = read_session_events(pf)
        session_data = extract_session_data(events)
        all_session_data.append(session_data)
    
    # Generate new merged session ID
    merged_session_id = str(uuid.uuid4())
    
    # Use standard naming convention: session_{uuid}.jsonl (for submit.py compatibility)
    output_prefix = f"session_{merged_session_id}"
    
    # Use earliest session_start for metadata
    first_session = all_session_data[0]
    last_session = all_session_data[-1]
    
    first_start = first_session.get('session_start') or {}
    last_end = last_session.get('session_end') or {}
    
    # Warn if sessions are incomplete
    missing_starts = sum(1 for sd in all_session_data if not sd.get('session_start'))
    missing_ends = sum(1 for sd in all_session_data if not sd.get('session_end'))
    if missing_starts > 0:
        print(f"  ⚠️  Warning: {missing_starts} session(s) missing session_start event")
    if missing_ends > 0:
        print(f"  ⚠️  Warning: {missing_ends} session(s) missing session_end event (incomplete sessions)")
    
    # Build merged session_start
    merged_start = {
        'type': 'session_start',
        'timestamp': first_start.get('timestamp', datetime.now(timezone.utc).isoformat()),
        'session_id': merged_session_id,
        'transcript_path': f"merged_from_{len(processed_files)}_sessions",
        'cwd': first_start.get('cwd', ''),
        'git_metadata': first_start.get('git_metadata'),
        'merged_from_sessions': [
            sd.get('session_start', {}).get('session_id', 'unknown') 
            for sd in all_session_data
        ]
    }
    
    # Copy A/B metadata from first session
    for key in ['task_id', 'model_lane', 'experiment_root', 'model_name']:
        if first_start.get(key):
            merged_start[key] = first_start[key]
    
    # Collect all messages and sort by timestamp
    all_messages = []
    for sd in all_session_data:
        for msg in sd['messages']:
            # Update session_id in each message
            msg_copy = msg.copy()
            msg_copy['session_id'] = merged_session_id
            msg_copy['original_session_id'] = msg.get('session_id', 'unknown')
            all_messages.append(msg_copy)
    
    all_messages.sort(key=lambda m: parse_timestamp(m.get('timestamp', '')))
    
    # Collect all session summaries for aggregation
    all_summaries = [sd['session_summary'] for sd in all_session_data if sd.get('session_summary')]
    aggregated_data = aggregate_summaries(all_summaries)
    
    # Build merged session_summary
    model_lane = first_start.get('model_lane', '')
    
    merged_summary = {
        'type': 'session_summary',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'session_id': merged_session_id,
        'transcript_path': f"merged_from_{len(processed_files)}_sessions",
        'cwd': first_start.get('cwd', ''),
        'summary_data': {
            **aggregated_data,
            'merged_sessions_count': len(processed_files),
            'original_session_ids': [
                sd.get('session_start', {}).get('session_id', 'unknown') 
                for sd in all_session_data
            ],
            'files': {
                'processed_log': f"{output_prefix}.jsonl",
                'raw_transcript': f"{output_prefix}_raw.jsonl",
                'git_diff': f"{model_lane}_diff.patch" if model_lane else None
            },
            'validation': {
                'complete': True,
                'unique_messages_processed': aggregated_data['total_messages'],
                'thinking_blocks_extracted': aggregated_data['thinking_metrics']['assistant_thinking_blocks_captured']
            }
        }
    }
    
    # Copy A/B metadata
    for key in ['task_id', 'model_lane', 'experiment_root', 'model_name']:
        if first_start.get(key):
            merged_summary[key] = first_start[key]
    
    # Build merged session_end
    merged_end = {
        'type': 'session_end',
        'timestamp': last_end.get('timestamp', datetime.now(timezone.utc).isoformat()),
        'session_id': merged_session_id,
        'transcript_path': f"merged_from_{len(processed_files)}_sessions",
        'cwd': last_end.get('cwd', first_start.get('cwd', '')),
        'reason': 'merged_sessions'
    }
    
    # Copy A/B metadata
    for key in ['task_id', 'model_lane', 'experiment_root', 'model_name']:
        if first_start.get(key):
            merged_end[key] = first_start[key]
    
    # Write merged processed log
    output_processed = logs_dir / f"{output_prefix}.jsonl"
    with open(output_processed, 'w', encoding='utf-8') as f:
        f.write(json.dumps(merged_start) + '\n')
        for msg in all_messages:
            f.write(json.dumps(msg) + '\n')
        f.write(json.dumps(merged_summary) + '\n')
        f.write(json.dumps(merged_end) + '\n')
    
    print(f"\n✓ Created merged processed log: {output_processed.name}")
    print(f"  - Session ID: {merged_session_id}")
    print(f"  - {len(all_messages)} messages from {len(processed_files)} sessions")
    
    # Merge raw transcripts if available
    if raw_files:
        output_raw = logs_dir / f"{output_prefix}_raw.jsonl"
        raw_event_count = 0
        
        with open(output_raw, 'w', encoding='utf-8') as f:
            for rf in raw_files:
                try:
                    with open(rf, 'r', encoding='utf-8') as src:
                        for line in src:
                            line = line.strip()
                            if line:
                                try:
                                    event = json.loads(line)
                                    # Update session_id
                                    if 'sessionId' in event:
                                        event['originalSessionId'] = event['sessionId']
                                        event['sessionId'] = merged_session_id
                                    f.write(json.dumps(event) + '\n')
                                    raw_event_count += 1
                                except:
                                    f.write(line + '\n')
                                    raw_event_count += 1
                except Exception as e:
                    print(f"Warning: Error reading {rf}: {e}", file=sys.stderr)
        
        print(f"✓ Created merged raw transcript: {output_raw.name}")
        print(f"  - {raw_event_count} events from {len(raw_files)} raw files")
    
    # Move original files to backup
    backup_dir = logs_dir / "original_sessions"
    backup_dir.mkdir(exist_ok=True)
    
    for pf in processed_files:
        shutil.move(str(pf), str(backup_dir / pf.name))
    for rf in raw_files:
        shutil.move(str(rf), str(backup_dir / rf.name))
    
    print(f"\n✓ Moved {len(processed_files) + len(raw_files)} original files to: {backup_dir.name}/")
    
    # Print summary
    print(f"\n" + "="*50)
    print("MERGED SESSION SUMMARY")
    print("="*50)
    print(f"New Session ID: {merged_session_id}")
    print(f"Total Duration: {aggregated_data['total_duration_seconds']:.1f} seconds")
    print(f"Total Messages: {aggregated_data['total_messages']}")
    print(f"  - Assistant: {aggregated_data['assistant_messages']}")
    print(f"  - User Prompts: {aggregated_data['user_prompts']}")
    print(f"Total Tokens:")
    ut = aggregated_data['usage_totals']
    print(f"  - Input: {ut['total_actual_input_tokens']:,}")
    print(f"  - Output: {ut['total_output_tokens']:,}")
    if aggregated_data['tool_metrics']['total_tool_calls'] > 0:
        print(f"Tool Calls: {aggregated_data['tool_metrics']['total_tool_calls']}")
    if aggregated_data['thinking_metrics']['assistant_thinking_blocks_captured'] > 0:
        print(f"Thinking Blocks: {aggregated_data['thinking_metrics']['assistant_thinking_blocks_captured']}")
    
    return True


def merge_experiment(experiment_root: Path) -> bool:
    """
    Merge sessions for both model_a and model_b in an experiment.
    
    Expects structure:
        experiment_root/
        ├── logs/
        │   ├── model_a/
        │   └── model_b/
    
    Returns True if at least one model was successfully merged.
    """
    logs_dir = experiment_root / "logs"
    
    if not logs_dir.exists():
        print(f"Error: logs/ directory not found in {experiment_root}", file=sys.stderr)
        return False
    
    results = {}
    
    for model in ["model_a", "model_b"]:
        model_logs = logs_dir / model
        
        if not model_logs.exists():
            print(f"\n⚠️  {model}: logs/{model}/ directory not found, skipping")
            results[model] = None
            continue
        
        # Count session files
        processed_files = list(model_logs.glob("session_*.jsonl"))
        processed_files = [f for f in processed_files if "_raw.jsonl" not in f.name]
        
        if len(processed_files) == 0:
            print(f"\n⚠️  {model}: No session files found, skipping")
            results[model] = None
        elif len(processed_files) == 1:
            print(f"\n✓ {model}: Already has exactly 1 session ({processed_files[0].name}), no merge needed")
            results[model] = True
        else:
            print(f"\n{'='*50}")
            print(f"Processing {model.upper()}")
            print(f"{'='*50}")
            print(f"Merging sessions in: {model_logs}")
            print("-" * 50)
            results[model] = merge_sessions(model_logs)
    
    # Summary
    print(f"\n{'='*50}")
    print("MERGE SUMMARY")
    print(f"{'='*50}")
    
    for model, result in results.items():
        if result is None:
            print(f"  {model}: ⚠️  Skipped (no sessions or directory missing)")
        elif result:
            print(f"  {model}: ✓ OK")
        else:
            print(f"  {model}: ❌ Failed")
    
    # Return True if both models are OK (either merged or already had 1 session)
    return all(r is not False for r in results.values())


def main():
    if len(sys.argv) < 2:
        print("Usage: python merge_sessions.py <path>")
        print("\nExamples:")
        print("  python merge_sessions.py .                    # from experiment root, fixes both models")
        print("  python merge_sessions.py /path/to/experiment  # same, with absolute path")
        print("  python merge_sessions.py ./logs/model_a       # merge only model_a sessions")
        print("\nWhen run from experiment root (with manifest.json), automatically checks")
        print("both logs/model_a and logs/model_b directories.")
        print("\nOriginal files will be moved to ./original_sessions/ in each logs folder.")
        sys.exit(1)
    
    target_dir = Path(sys.argv[1]).resolve()
    
    if not target_dir.exists():
        print(f"Error: Directory not found: {target_dir}", file=sys.stderr)
        sys.exit(1)
    
    if not target_dir.is_dir():
        print(f"Error: Not a directory: {target_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Check if this is an experiment root (has manifest.json or logs/ folder)
    is_experiment_root = (target_dir / "manifest.json").exists() or (target_dir / "logs").exists()
    
    if is_experiment_root:
        print(f"Detected experiment root: {target_dir}")
        print("Checking both model_a and model_b logs...")
        success = merge_experiment(target_dir)
    else:
        # Direct logs directory (e.g., logs/model_a)
        print(f"Merging sessions in: {target_dir}")
        print("-" * 50)
        success = merge_sessions(target_dir)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
