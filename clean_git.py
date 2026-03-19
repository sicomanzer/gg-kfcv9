import shutil
import os

try:
    if os.path.exists('.git/rebase-merge'):
        shutil.rmtree('.git/rebase-merge')
        print("Removed .git/rebase-merge")
    else:
        print(".git/rebase-merge not found")
        
    if os.path.exists('unlock_git.py'):
        # Self-deletion might be tricky on Windows if file is open, but usually ok in script?
        # Better to delete it separately or just ignore it.
        pass
except Exception as e:
    print(f"Error: {e}")
