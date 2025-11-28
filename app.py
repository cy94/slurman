from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import yaml
import subprocess
import json
import os
import re
from pathlib import Path
from datetime import datetime, timedelta

COMMENTS_PATH = Path(__file__).parent / 'comments.json'
DEFAULT_CHUNK_SIZE = 200_000
MAX_CHUNK_SIZE = 1_000_000
MIN_CHUNK_SIZE = 8_192
FAILED_LOOKBACK_HOURS = 72
COMPLETED_LOOKBACK_HOURS = 72
FAILED_STATE_CODES = ['FAILED', 'OUT_OF_MEMORY', 'TIMEOUT', 'CANCELLED', 'NODE_FAIL']
SACCT_TIMEOUT_SECONDS = 20
JOB_ID_PATTERN = re.compile(r'(?<!\d)(\d{5,})(?!\d)')

app = Flask(__name__, static_folder='static')
CORS(app)

def load_config():
    """Load configuration from config.yaml"""
    config_path = Path(__file__).parent / 'config.yaml'
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_output_dir():
    config = load_config()
    path_value = config.get('log_dir')
    if not path_value:
        raise ValueError("log_dir must be set in config.yaml")
    return Path(path_value).expanduser()

def get_slurm_jobs(username):
    """Query slurm jobs for a specific user using squeue"""
    try:
        # Use squeue to get running and pending jobs with formatted output
        # Format: JOBID|PARTITION|NAME|USER|STATE|TIME|NODES|REASON|TIME_LIMIT|START_TIME|SUBMIT_TIME
        cmd = ['squeue', '-u', username, '--format', 
               '%.18i|%.9P|%.20j|%.8u|%.8T|%.10M|%.6D|%.20R|%.10l|%.20S|%.20V', '--noheader']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        
        if result.returncode != 0:
            # If --noheader fails, try without it and parse manually
            cmd = ['squeue', '-u', username, '--format', 
                   '%.18i|%.9P|%.20j|%.8u|%.8T|%.10M|%.6D|%.20R|%.10l|%.20S|%.20V']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            
            if result.returncode != 0:
                return []
            
            lines = result.stdout.strip().split('\n')
            if len(lines) <= 1:  # Only header or empty
                return []
            # Skip header line
            lines = lines[1:]
        else:
            lines = result.stdout.strip().split('\n')
            if not lines or (len(lines) == 1 and not lines[0].strip()):
                return []
        
        # Field names matching the format string
        field_names = ['JOBID', 'PARTITION', 'NAME', 'USER', 'STATE', 'TIME', 
                      'NODES', 'REASON', 'TIME_LIMIT', 'START_TIME', 'SUBMIT_TIME']
        
        jobs = []
        for line in lines:
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= len(field_names):
                job = {}
                for i, field in enumerate(field_names):
                    if i < len(parts):
                        job[field] = parts[i] if parts[i] else '-'
                jobs.append(job)
        
        return jobs
    except subprocess.TimeoutExpired:
        print("Timeout querying slurm jobs")
        return []
    except Exception as e:
        print(f"Error querying slurm jobs: {e}")
        return []

def find_log_file(job_id):
    """Find the most recent log file containing the job ID"""
    try:
        output_dir = get_output_dir()
    except ValueError as exc:
        print(f"Config error while locating log dir: {exc}")
        return None
    if output_dir is None or not output_dir.exists():
        return None

    job_str = str(job_id)
    candidates = []

    # Search non-recursively first for speed
    for pattern in (f'*{job_str}*.log', f'*{job_str}*'):
        matches = list(output_dir.glob(pattern))
        if matches:
            candidates.extend(matches)
            break

    # Fallback to recursive search if nothing found
    if not candidates:
        for pattern in (f'*{job_str}*.log', f'*{job_str}*'):
            matches = list(output_dir.rglob(pattern))
            if matches:
                candidates.extend(matches)
                break

    if not candidates:
        return None

    # Return most recently modified file
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def read_log_chunk(file_path, start=None, length=None):
    """Read a chunk of the log file"""
    try:
        chunk_size = length or DEFAULT_CHUNK_SIZE
        chunk_size = max(MIN_CHUNK_SIZE, min(chunk_size, MAX_CHUNK_SIZE))
        size = file_path.stat().st_size
        if start is None:
            start = max(size - chunk_size, 0)
        else:
            start = max(0, min(start, size))
        end = min(start + chunk_size, size)
        with open(file_path, 'rb') as f:
            f.seek(start)
            data = f.read(end - start)
        text = data.decode('utf-8', errors='replace')
        has_prev = start > 0
        has_next = end < size
        prev_start = max(start - chunk_size, 0) if has_prev else None
        next_start = end if has_next else None
        return {
            'content': text,
            'start': start,
            'end': end,
            'size_bytes': size,
            'chunk_bytes': chunk_size,
            'has_prev': has_prev,
            'has_next': has_next,
            'prev_start': prev_start,
            'next_start': next_start
        }
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"Error reading log file {file_path}: {e}")
        return None

def load_comments():
    """Load stored comments from JSON file"""
    if not COMMENTS_PATH.exists():
        return {}
    try:
        with open(COMMENTS_PATH, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except json.JSONDecodeError:
        print("Warning: comments.json could not be decoded; starting fresh.")
    except Exception as e:
        print(f"Error loading comments: {e}")
    return {}

def save_comments(comments):
    """Persist comments to JSON file"""
    try:
        COMMENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(COMMENTS_PATH, 'w') as f:
            json.dump(comments, f, indent=2)
    except Exception as e:
        print(f"Error saving comments: {e}")

def cancel_slurm_job(job_id):
    """Cancel a specific slurm job using scancel"""
    if not str(job_id).isdigit():
        return False, "Invalid job ID"

    try:
        cmd = ['scancel', str(job_id)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)

        if result.returncode != 0:
            error_message = result.stderr.strip() or "Failed to cancel job"
            return False, error_message

        return True, "Job cancellation requested"
    except subprocess.TimeoutExpired:
        return False, "Timeout while attempting to cancel job"
    except Exception as e:
        return False, f"Error cancelling job: {e}"

def get_job_script(job_id):
    """Fetch the batch script used to submit the job"""
    try:
        cmd = ['scontrol', 'write', 'batch_script', str(job_id), '-']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            error_message = result.stderr.strip() or "Batch script not available"
            return False, error_message
        content = result.stdout.strip()
        if not content:
            return False, "Batch script not available"
        return True, content
    except subprocess.TimeoutExpired:
        return False, "Timeout while fetching batch script"
    except Exception as e:
        return False, f"Error fetching batch script: {e}"

def get_job_info_details(job_id):
    """Fetch detailed job info via scontrol"""
    try:
        cmd = ['scontrol', 'show', 'jobid', '-dd', str(job_id)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            error_message = result.stderr.strip() or "Job info not available"
            return False, error_message
        content = result.stdout.strip()
        if not content:
            return False, "Job info not available"
        parsed = parse_scontrol_output(content)
        return True, {'raw': content, 'parsed': parsed}
    except subprocess.TimeoutExpired:
        return False, "Timeout while fetching job info"
    except Exception as e:
        return False, f"Error fetching job info: {e}"

def parse_scontrol_output(text):
    """Parse scontrol key=value output into a dict"""
    info = {}
    if not text:
        return info
    tokens = text.replace('\n', ' ').split()
    for token in tokens:
        if '=' not in token:
            continue
        key, value = token.split('=', 1)
        info[key.strip()] = value.strip().strip('"')
    return info

def search_log_files(query, max_results=20):
    """Search log files by job id or partial filename"""
    output_dir = get_output_dir()
    if not query or not output_dir.exists():
        return []
    query = query.strip()
    q_lower = query.lower()
    results = []

    # if query looks like job id (digits) prioritize exact match
    is_job_id = query.isdigit()
    if is_job_id:
        path = find_log_file(query)
        if path:
            results.append(path)

    if len(results) < max_results:
        try:
            for path in output_dir.rglob('*'):
                if not path.is_file():
                    continue
                name_lower = path.name.lower()
                if q_lower in name_lower:
                    if path not in results:
                        results.append(path)
                    if len(results) >= max_results:
                        break
        except Exception as e:
            print(f"Error searching log files: {e}")

    formatted = []
    for path in results[:max_results]:
        try:
            stat = path.stat()
            formatted.append({
                'filename': path.name,
                'path': str(path),
                'relative_path': str(path.relative_to(output_dir)),
                'size_bytes': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
        except Exception as e:
            print(f"Error reading file info for {path}: {e}")
    return formatted

def _build_sacct_recent_jobs_cmd(username, hours, fields_base, default_hours):
    """Internal helper to build sacct command for recent jobs."""
    lookback_hours = max(1, min(int(hours or default_hours), 24 * 14))
    # Always include End time in the fields so we can sort by completion time
    fields = f'{fields_base},End'
    since = datetime.utcnow() - timedelta(hours=lookback_hours)
    cmd = [
        'sacct',
        '-X',  # hide steps to keep output small/fast
        '--parsable2',
        '--noheader',
        f'--format={fields}',
        '-u', username,
        f'--starttime={since.strftime("%Y-%m-%dT%H:%M:%S")}',
    ]
    return cmd, lookback_hours


def get_recent_failed_jobs(username, hours=FAILED_LOOKBACK_HOURS, max_results=20):
    """Fetch recently failed jobs via sacct. Returns (jobs, error)."""
    fields_base = 'JobID,JobName,State,ExitCode'
    cmd, _ = _build_sacct_recent_jobs_cmd(
        username,
        hours,
        fields_base=fields_base,
        default_hours=FAILED_LOOKBACK_HOURS,
    )
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=SACCT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        message = "Timeout querying recent failed jobs via sacct"
        print(message)
        return [], message
    except Exception as exc:
        message = f"Error querying recent failed jobs: {exc}"
        print(message)
        return [], message

    if result.returncode != 0:
        stderr = result.stderr.strip() or "Unknown sacct error"
        message = f"sacct returned non-zero exit code: {stderr}"
        print(message)
        return [], message

    jobs = []
    seen_ids = set()
    lines = result.stdout.strip().splitlines()
    for line in lines:
        if not line.strip():
            continue
        parts = line.split('|')
        if len(parts) < 4:
            continue
        job_id_full = parts[0].strip()
        if not job_id_full:
            continue
        job_id = job_id_full.split('.')[0]
        if not job_id or job_id in seen_ids:
            continue
        job_name = parts[1].strip()
        state = parts[2].strip()
        if state.upper() != 'FAILED':
            continue
        exit_code = parts[3].strip()
        end_time = parts[4].strip() if len(parts) > 4 else ''
        seen_ids.add(job_id)
        jobs.append({
            'JOBID': job_id,
            'NAME': job_name,
            'STATE': state,
            'EXIT_CODE': exit_code,
            'END_TIME': end_time
        })

    jobs.sort(key=lambda j: j.get('END_TIME') or '', reverse=True)
    limited = jobs[:max(1, max_results)]
    return limited, None


def get_recent_completed_jobs(username, hours=COMPLETED_LOOKBACK_HOURS, max_results=20):
    """Fetch recently successfully completed jobs via sacct. Returns (jobs, error)."""
    fields_base = 'JobID,JobName,State,ExitCode'
    cmd, _ = _build_sacct_recent_jobs_cmd(
        username,
        hours,
        fields_base=fields_base,
        default_hours=COMPLETED_LOOKBACK_HOURS,
    )
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=SACCT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        message = "Timeout querying recent completed jobs via sacct"
        print(message)
        return [], message
    except Exception as exc:
        message = f"Error querying recent completed jobs: {exc}"
        print(message)
        return [], message

    if result.returncode != 0:
        stderr = result.stderr.strip() or "Unknown sacct error"
        message = f"sacct returned non-zero exit code: {stderr}"
        print(message)
        return [], message

    jobs = []
    seen_ids = set()
    lines = result.stdout.strip().splitlines()
    for line in lines:
        if not line.strip():
            continue
        parts = line.split('|')
        if len(parts) < 4:
            continue
        job_id_full = parts[0].strip()
        if not job_id_full:
            continue
        job_id = job_id_full.split('.')[0]
        if not job_id or job_id in seen_ids:
            continue
        job_name = parts[1].strip()
        state = parts[2].strip()
        state_upper = state.upper()
        if not state_upper.startswith('COMPLETED'):
            continue
        exit_code = parts[3].strip()
        end_time = parts[4].strip() if len(parts) > 4 else ''
        seen_ids.add(job_id)
        jobs.append({
            'JOBID': job_id,
            'NAME': job_name,
            'STATE': state,
            'EXIT_CODE': exit_code,
            'END_TIME': end_time
        })

    jobs.sort(key=lambda j: j.get('END_TIME') or '', reverse=True)
    limited = jobs[:max(1, max_results)]
    return limited, None

def extract_job_id(value, fallback=None):
    """Best-effort extraction of a numeric job id from text"""
    if value:
        match = JOB_ID_PATTERN.findall(str(value))
        if match:
            # Prefer the longest numeric token to reduce false positives
            return max(match, key=len)
    if fallback and str(fallback).isdigit():
        return str(fallback)
    return None

def get_log_by_relative_path(relative_path, start=None, length=None):
    """Read a log chunk by its relative path under the configured directory"""
    try:
        output_dir = get_output_dir()
    except ValueError as exc:
        return False, str(exc)

    try:
        target = (output_dir / relative_path).resolve()
        if not str(target).startswith(str(output_dir.resolve())):
            return False, "Invalid path"
        if not target.exists() or not target.is_file():
            return False, "Log file not found"
        chunk = read_log_chunk(target, start=start, length=length)
        if chunk is None:
            return False, "Unable to read log file"
        chunk['path'] = str(target)
        chunk['relative_path'] = relative_path
        return True, chunk
    except Exception as e:
        return False, f"Error accessing log file: {e}"

@app.route('/')
def index():
    """Serve the main HTML page"""
    return send_from_directory('static', 'index.html')

@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    """API endpoint to get slurm jobs"""
    config = load_config()
    username = config.get('slurm_username')
    jobs = get_slurm_jobs(username)
    comments = load_comments()
    for job in jobs:
        job_id = job.get('JOBID')
        if job_id:
            job['comment'] = comments.get(str(job_id), '')
        else:
            job['comment'] = ''
    return jsonify(jobs)

@app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """API endpoint to cancel a specific slurm job"""
    success, message = cancel_slurm_job(job_id)
    status_code = 200 if success else 400
    return jsonify({'success': success, 'message': message}), status_code

@app.route('/api/jobs/<job_id>/log', methods=['GET'])
def get_job_log(job_id):
    """API endpoint to fetch the full contents of a job's log file"""
    start = request.args.get('start', default=None, type=int)
    length = request.args.get('length', default=DEFAULT_CHUNK_SIZE, type=int)
    log_file = find_log_file(job_id)
    if not log_file:
        try:
            log_dir = get_output_dir()
        except ValueError as exc:
            return jsonify({'success': False, 'message': str(exc)}), 500
        return jsonify({'success': False, 'message': f'No log file found for job {job_id} in {log_dir}'}), 404

    chunk = read_log_chunk(log_file, start=start, length=length)
    if chunk is None:
        return jsonify({'success': False, 'message': f'Unable to read log file for job {job_id}'}), 500

    return jsonify({
        'success': True,
        'log': chunk['content'],
        'path': str(log_file),
        'size_bytes': chunk['size_bytes'],
        'start': chunk['start'],
        'end': chunk['end'],
        'has_prev': chunk['has_prev'],
        'has_next': chunk['has_next'],
        'prev_start': chunk['prev_start'],
        'next_start': chunk['next_start'],
        'chunk_bytes': chunk['chunk_bytes']
    })

@app.route('/api/jobs/<job_id>/comment', methods=['GET'])
def get_job_comment(job_id):
    """Fetch the stored comment for a job"""
    comments = load_comments()
    comment = comments.get(str(job_id), '')
    return jsonify({'success': True, 'job_id': str(job_id), 'comment': comment})

@app.route('/api/jobs/<job_id>/comment', methods=['POST'])
def set_job_comment(job_id):
    """Store or update a comment for a job"""
    payload = request.get_json(silent=True) or {}
    comment = payload.get('comment', '').strip()
    comments = load_comments()
    comments[str(job_id)] = comment
    save_comments(comments)
    return jsonify({'success': True, 'job_id': str(job_id), 'comment': comment})

@app.route('/api/jobs/<job_id>/script', methods=['GET'])
def get_job_script_endpoint(job_id):
    """Return the batch script used for the job"""
    success, content = get_job_script(job_id)
    status_code = 200 if success else 404
    return jsonify({'success': success, 'job_id': str(job_id), 'script': content}), status_code

@app.route('/api/jobs/<job_id>/info', methods=['GET'])
def get_job_info_endpoint(job_id):
    """Return detailed info about the job"""
    success, content = get_job_info_details(job_id)
    status_code = 200 if success else 404
    if success:
        payload = {
            'success': True,
            'job_id': str(job_id),
            'info': content.get('raw', ''),
            'details': content.get('parsed', {})
        }
    else:
        payload = {'success': False, 'job_id': str(job_id), 'info': content}
    return jsonify(payload), status_code

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok'})

@app.route('/api/logs/search', methods=['GET'])
def search_logs():
    """Search historical log files by job id or filename"""
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'success': True, 'results': []})
    try:
        results = search_log_files(query)
        comments = load_comments()
        annotated = []
        for result in results:
            job_id = extract_job_id(result.get('filename') or result.get('path'), fallback=query if query.isdigit() else None)
            comment = comments.get(job_id, '') if job_id else ''
            entry = dict(result)
            entry['job_id'] = job_id
            entry['comment'] = comment
            annotated.append(entry)
    except ValueError as exc:
        return jsonify({'success': False, 'message': str(exc), 'results': []}), 400
    return jsonify({'success': True, 'results': annotated})

@app.route('/api/jobs/failed', methods=['GET'])
def get_failed_jobs():
    """Return recently failed jobs along with stored comments"""
    config = load_config()
    username = config.get('slurm_username')
    hours = request.args.get('hours', default=FAILED_LOOKBACK_HOURS, type=int)
    limit = request.args.get('limit', default=20, type=int)
    jobs, error = get_recent_failed_jobs(username, hours=hours, max_results=limit)
    if error:
        return jsonify({'success': False, 'message': error, 'jobs': []}), 500
    comments = load_comments()
    for job in jobs:
        job_id = job.get('JOBID')
        job['comment'] = comments.get(str(job_id), '') if job_id else ''
    return jsonify({'success': True, 'jobs': jobs, 'hours': hours, 'count': len(jobs)})


@app.route('/api/jobs/completed', methods=['GET'])
def get_completed_jobs():
    """Return recently completed jobs along with stored comments"""
    config = load_config()
    username = config.get('slurm_username')
    hours = request.args.get('hours', default=COMPLETED_LOOKBACK_HOURS, type=int)
    limit = request.args.get('limit', default=20, type=int)
    jobs, error = get_recent_completed_jobs(username, hours=hours, max_results=limit)
    if error:
        return jsonify({'success': False, 'message': error, 'jobs': []}), 500
    comments = load_comments()
    for job in jobs:
        job_id = job.get('JOBID')
        job['comment'] = comments.get(str(job_id), '') if job_id else ''
    return jsonify({'success': True, 'jobs': jobs, 'hours': hours, 'count': len(jobs)})

@app.route('/api/logs/view', methods=['GET'])
def view_log_by_path():
    """Return log contents by relative path"""
    relative_path = request.args.get('path', '').strip()
    if not relative_path:
        return jsonify({'success': False, 'message': 'Path is required'}), 400
    start = request.args.get('start', default=None, type=int)
    length = request.args.get('length', default=DEFAULT_CHUNK_SIZE, type=int)
    success, result = get_log_by_relative_path(relative_path, start=start, length=length)
    status_code = 200 if success else 404
    if success:
        payload = {'success': True, **result}
    else:
        payload = {'success': False, 'message': result}
    return jsonify(payload), status_code

@app.route('/api/config', methods=['GET'])
def get_config():
    """Return basic configuration info"""
    config = load_config() or {}
    response = dict(config)
    try:
        response['log_dir'] = str(get_output_dir())
        response['log_dir_error'] = None
    except ValueError as exc:
        response['log_dir'] = ''
        response['log_dir_error'] = str(exc)
    return jsonify(response)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)

