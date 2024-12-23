import time
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
plt.style.use('seaborn-v0_8')

def get_folder_size(folder_path):
    total_size = 0
    for path in Path(folder_path).rglob('*'):
        if path.is_file():
            total_size += path.stat().st_size
    return total_size / (1024 * 1024)  # Convert to MB

def monitor_folder_size(folder_path, update_interval=5):
    sizes = []
    times = []
    
    plt.ion()  # Enable interactive plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    line, = ax.plot(times, sizes)
    
    ax.set_title('Folder Size Over Time')
    ax.set_xlabel('Time')
    ax.set_ylabel('Size (MB)')
    
    try:
        while True:
            current_size = get_folder_size(folder_path)
            current_time = datetime.now().strftime('%H:%M:%S')
            
            sizes.append(current_size)
            times.append(current_time)
            
            # Keep last 50 measurements
            if len(sizes) > 50:
                sizes.pop(0)
                times.pop(0)
            
            # Update plot
            line.set_xdata(range(len(sizes)))
            line.set_ydata(sizes)
            ax.set_xticks(range(len(times)))
            ax.set_xticklabels(times, rotation=45)
            ax.relim()
            ax.autoscale_view()
            fig.canvas.draw()
            fig.canvas.flush_events()
            
            print(f"\rCurrent folder size: {current_size:.2f} MB", end='')
            time.sleep(update_interval)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped")
        plt.ioff()

if __name__ == "__main__":
    folder_path = "./processed_data"  # Adjust this path as needed
    monitor_folder_size(folder_path)