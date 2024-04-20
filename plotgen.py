import subprocess
import sys

# Path to the virtual environment's activate script
venv_activate = 'thesis_env\\Scripts\\activate.bat'

# List of Python scripts to run
scripts = [
    'gasused_area_block_avg.py',
    'gasused_area.py',
    'gasused_plot_block_avg.py',
    'gasused_plot.py',
    'tx_count_area_block_avg.py',
    'tx_count_area.py',
    'tx_count_plot_block_avg.py',
    'tx_count_plot.py'
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