#!/usr/bin/env python3
"""
Claude Code A/B Testing Init Script (Cross-Platform)
Downloads and executes the setup script.
Works on Windows, macOS, and Linux.
"""
import os
import sys
import subprocess
import platform
import zipfile
from pathlib import Path

# Check Python version first
if sys.version_info < (3, 7):
    print("[ERROR] Python 3.7 or higher is required")
    print(f"[ERROR] Current version: {sys.version}")
    sys.exit(1)

# Detect platform
IS_WINDOWS = platform.system() == 'Windows'

# Supabase configuration
# Note: requests import is deferred until after venv setup option
SUPABASE_URL = "https://sdippjgffrptdvlmlurv.supabase.co"
ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkaXBwamdmZnJwdGR2bG1sdXJ2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTk4MTg4MzAsImV4cCI6MjA3NTM5NDgzMH0.f8zJ4fIcZFmzpRpngQ6NWIUudbBptGIO2vb5GBWfc2A"

def print_error(message):
    """Print error message and exit."""
    prefix = "[ERROR]" if IS_WINDOWS else "❌ ERROR:"
    print(f"{prefix} {message}", file=sys.stderr)
    sys.exit(1)

def print_success(message):
    """Print success message."""
    prefix = "[OK]" if IS_WINDOWS else "✅"
    print(f"{prefix} {message}")

def print_info(message):
    """Print info message."""
    prefix = "[INFO]" if IS_WINDOWS else "ℹ️ "
    print(f"{prefix} {message}")

def create_repository_snapshot_zip(source_dir, zip_file_path):
    """Create a zip file of the repository, excluding system files and .git/.claude directories."""
    try:
        if os.path.exists(zip_file_path):
            os.remove(zip_file_path)

        exclude_patterns = {'.git', '.claude', '.DS_Store', '__pycache__', '.vscode', '.idea',
                          'node_modules', '.pytest_cache', '.mypy_cache', '*.pyc', '*.pyo'}

        def should_exclude(file_path):
            path_parts = Path(file_path).parts
            name = os.path.basename(file_path)
            for part in path_parts:
                if part in exclude_patterns or part.startswith('.'):
                    if part not in {'.gitignore', '.env.example', '.dockerignore'}:
                        return True
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
        print(f"Error creating snapshot: {e}", file=sys.stderr)
        return False

def take_start_snapshots():
    """Take initial snapshots of both model directories."""
    snapshots_dir = Path("snapshots")
    snapshots_dir.mkdir(exist_ok=True)

    model_dirs = {"model_a": Path("model_a"), "model_b": Path("model_b")}

    for model_lane, model_dir in model_dirs.items():
        if not model_dir.exists():
            print_info(f"Skipping snapshot for {model_lane} - directory not found")
            continue

        snapshot_zip = snapshots_dir / f"{model_lane}_start.zip"
        if create_repository_snapshot_zip(str(model_dir), str(snapshot_zip)):
            print_success(f"Created start snapshot for {model_lane}")
        else:
            print_error(f"Failed to create start snapshot for {model_lane}")

    return True

def download_setup_script():
    """Download setup.py from Supabase."""
    # Import requests here (after venv setup)
    try:
        import requests
    except ImportError:
        print_error("'requests' library not found. This should have been installed in venv.")
        return False
    
    url = f"{SUPABASE_URL}/storage/v1/object/code-preferences-setup-files-v2/setup.py"
    headers = {
        "Authorization": f"Bearer {ANON_KEY}",
        "apikey": ANON_KEY
    }
    
    try:
        print_info("Downloading setup script...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        setup_path = Path("setup.py")
        with open(setup_path, 'wb') as f:
            f.write(response.content)
        
        # Make setup script executable (Unix/Mac only)
        if not IS_WINDOWS:
            os.chmod(setup_path, 0o755)
        
        print_success("Setup script downloaded")
        return True
        
    except requests.exceptions.RequestException as e:
        print_error(f"Failed to download setup script: {e}")
        return False

def run_setup_script():
    """Execute the downloaded setup script."""
    try:
        print_info("Running setup script...")
        
        # Use the same Python interpreter that's running this script
        result = subprocess.run(
            [sys.executable, "setup.py"],
            check=False
        )
        
        return result.returncode == 0
        
    except Exception as e:
        print_error(f"Failed to run setup script: {e}")
        return False

def check_python_command():
    """Verify 'python' command is globally available."""
    try:
        result = subprocess.run(['python', '--version'], 
                              capture_output=True, timeout=5)
        if result.returncode == 0:
            version_output = result.stdout.decode() or result.stderr.decode()
            print_success(f"Python command found: {version_output.strip()}")
            return True
    except Exception:
        pass
    
    # If python doesn't work, show fix instructions
    prefix = "[ERROR]" if IS_WINDOWS else "❌"
    print(f"{prefix} Python installation not configured correctly", file=sys.stderr)
    print("")
    print("[FIX] Make 'python' command work:")
    print("")
    
    if IS_WINDOWS:
        print("  Reinstall Python with 'Add Python to PATH' checked")
        print("  Download: https://www.python.org/downloads/")
    else:  # Mac/Linux
        print("  Create symlink: sudo ln -s $(which python3) /usr/local/bin/python")
        print("  Then restart your terminal")
    
    print("")
    print("Python must be globally accessible for hooks to work.")
    sys.exit(1)

def check_dependencies():
    """Check for required system dependencies."""
    issues = []
    warnings = []
    
    # Check Python version (already verified it runs via check_python_command)
    if sys.version_info < (3, 7):
        issues.append(f"Python 3.7+ required (current: {sys.version})")
    
    # Check git
    try:
        result = subprocess.run(['git', '--version'], capture_output=True, timeout=5)
        if result.returncode != 0:
            issues.append("Git not found or not working")
    except Exception:
        issues.append("Git not installed")
    
    # Check Python libraries
    try:
        import requests
    except ImportError:
        warnings.append("'requests' not installed (needed for setup.py)")
    
    try:
        import supabase
    except ImportError:
        warnings.append("'supabase' not installed (needed for submit.py)")
    
    try:
        import tusclient
    except ImportError:
        warnings.append("'tusclient' (tuspy) not installed (needed for submit.py)")
    
    return issues, warnings

def setup_virtual_environment():
    """Optionally create virtual environment and install dependencies."""
    try:
        print("\n" + "=" * 50)
        print("Virtual Environment Setup (Optional)")
        print("=" * 50)
        print("[INFO] Would you like to create a virtual environment?")
        print("  This will:")
        print("  - Create .venv folder")
        print("  - Install: requests, supabase, tuspy")
        print("  - Keep dependencies isolated")
        print("")
        
        choice = input("Create virtual environment? (Y/n): ").strip().lower()
        
        if choice not in ['', 'y', 'yes']:
            print_info("Skipping virtual environment setup")
            return None
        
        # Create venv
        print_info("Creating virtual environment (.venv)...")
        result = subprocess.run([sys.executable, '-m', 'venv', '.venv'], 
                              capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            print_error(f"Failed to create virtual environment: {result.stderr}")
            return None
        
        print_success("Virtual environment created")
        
        # Detect venv python and pip paths
        venv_dir = Path('.venv')
        if IS_WINDOWS:
            venv_python = venv_dir / 'Scripts' / 'python.exe'
            venv_pip = venv_dir / 'Scripts' / 'pip.exe'
        else:
            venv_python = venv_dir / 'bin' / 'python'
            venv_pip = venv_dir / 'bin' / 'pip'
        
        if not venv_python.exists():
            print_error(f"Virtual environment creation succeeded but python not found at {venv_python}")
            return None
        
        # Upgrade pip first
        print_info("Upgrading pip...")
        subprocess.run([str(venv_pip), 'install', '--upgrade', 'pip'], 
                      capture_output=True, timeout=60)
        
        # Install dependencies
        print_info("Installing dependencies (requests, supabase, tuspy)...")
        result = subprocess.run(
            [str(venv_pip), 'install', 'requests', 'supabase', 'tuspy'],
            capture_output=True, text=True, timeout=120
        )
        
        if result.returncode != 0:
            print_error(f"Failed to install dependencies: {result.stderr}")
            return None
        
        print_success("Dependencies installed in virtual environment")
        
        # Exit and ask user to re-run with venv
        print("\n" + "=" * 50)
        print_success("Virtual environment created successfully!")
        print("=" * 50)
        print("")
        print("[IMPORTANT] Next step:")
        print("")
        if IS_WINDOWS:
            print("  .venv\\Scripts\\python.exe init.py")
        else:
            print("  .venv/bin/python init.py")
        print("")
        print("Copy and run the command above to continue setup with the venv.")
        print("=" * 50)
        
        # Exit gracefully - don't continue with system Python
        return "VENV_CREATED_EXIT"  # Signal to exit in main()
        
    except KeyboardInterrupt:
        print_info("\nSkipping virtual environment setup")
        return None
    except Exception as e:
        print_info(f"Virtual environment setup failed: {e}")
        print_info("Continuing without virtual environment...")
        return None

def main():
    """Main init function."""
    platform_name = "Windows" if IS_WINDOWS else platform.system()
    title = f"Claude Code A/B Testing Init ({platform_name})" if IS_WINDOWS else "🚀 Claude Code A/B Testing Init"
    print(title)
    print("=" * 50)
    
    try:
        # FIRST: Check 'python' command is available (critical for hooks)
        print_info("Verifying Python installation...")
        check_python_command()
        
        # Check other dependencies
        print_info("Checking other dependencies...")
        issues, warnings = check_dependencies()
        
        if issues:
            prefix = "[ERROR]" if IS_WINDOWS else "❌"
            print(f"{prefix} Missing required dependencies:")
            for issue in issues:
                print(f"  - {issue}", file=sys.stderr)
            print("\n[INFO] Please install:")
            print("  - Python 3.7+: https://www.python.org/downloads/")
            print("  - Git: https://git-scm.com/downloads")
            print("  - requests: pip install requests")
            sys.exit(1)
        
        print_success("Core system dependencies found (Python, Git)")
        
        # Offer to create virtual environment if any Python packages missing
        if warnings:
            print("")
            for warning in warnings:
                print_info(warning)
            
            # Offer venv setup - exits if created
            result = setup_virtual_environment()
            
            if result == "VENV_CREATED_EXIT":
                # Venv was created, script already printed re-run instructions
                sys.exit(0)
            
            # If we reach here, user declined venv
            print("\n[INFO] To install dependencies manually:")
            print("  pip install requests supabase tuspy")
            print("  OR")
            print("  pipx install requests supabase tuspy")
            print("")
            
            # Check if requests is available (critical for setup.py)
            try:
                import requests
            except ImportError:
                print_error("'requests' library is required to continue. Please install it and run again.")
        else:
            print_success("All Python dependencies found")
        
        # Download setup script (requests imported inside function)
        if not download_setup_script():
            print_error("Failed to download setup script")
        
        # Run setup script
        if not run_setup_script():
            print_error("Setup script failed")

        # Take initial snapshots after setup completes
        print_info("Taking initial repository snapshots...")
        take_start_snapshots()

    except KeyboardInterrupt:
        prefix = "[CANCELLED]" if IS_WINDOWS else "❌"
        print(f"\n{prefix} Init cancelled by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error during init: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

