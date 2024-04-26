import subprocess
import sys

# Path to the virtual environment's activate script
venv_activate = 'thesis_env\\Scripts\\activate.bat'

# List of Python scripts to run
scripts = [
    'count_bar_without_swap.py',
    'count_mev_types_block.py',
    'count_mev_types_with_swap_block.py',
    'count_mev_types_with_swap.py',
    'count_mev_types.py',
    'count_plot_mev_types_with_swap.py',
    
    'pct_mev_types_block.py',
    'pct_mev_types_with_swap.py',
    'pct_mev_types.py',
    
    'private_mev_ratio_block.py',
    'private_mev_ratio_equal_scale_block.py',
    'private_mev_ratio_equal_scale.py',
    'private_mev_ratio.py',
    
]

def run_script(script_name):
    """Run a Python script using the activated virtual environment."""
    try:
        # Running the Python script
        subprocess.check_call([sys.executable, script_name])
        print(f"{script_name} executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing {script_name}: {e}")

if __name__ == '__main__':
    # Activate the virtual environment
    subprocess.call(venv_activate, shell=True)

    # Execute each script
    for script in scripts:
        run_script(script)