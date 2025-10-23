# main.py
import subprocess

def run_generate_network():
    subprocess.run(["python", "DB_StarterPack/generate_network.py"])

def run_influence_analysis():
    subprocess.run(["python", "DB_StarterPack/influence_analysis.py"])

if __name__ == "__main__":
    print("\n=== Social Network Project ===")
    print("1 - Generate New Network")
    print("2 - Run Influence Analysis on Existing Network")

    choice = input("\nEnter choice: ").strip()
    if choice == "1":
        run_generate_network()
    elif choice == "2":
        run_influence_analysis()
    else:
        print("❌ Invalid choice.")
