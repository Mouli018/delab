"""
Data Engineering Laboratory (22MDCEL10) — Test Suite Runner
================================================================
Executes pytest across all week modules (Week 1 through Week 5)
and integration tests.

Usage:
  python run_tests.py
"""
import sys
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent

def run_tests():
    print("=" * 70)
    print("[TEST SUITE] Running Data Engineering Laboratory Tests (Weeks 1 - 5)")
    print("=" * 70)
    
    cmd = [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"]
    result = subprocess.run(cmd, cwd=str(BASE))
    
    if result.returncode == 0:
        print("\n" + "=" * 70)
        print("[SUCCESS] All 23 test cases passed cleanly!")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("[FAILURE] Test suite encountered errors.")
        print("=" * 70)
    
    sys.exit(result.returncode)

if __name__ == "__main__":
    run_tests()
