import os
try:
    os.remove('.git/index.lock')
    print("Deleted .git/index.lock")
except OSError as e:
    print(f"Error: {e}")
