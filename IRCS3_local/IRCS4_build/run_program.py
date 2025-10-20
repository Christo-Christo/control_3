from syntax.main import main
import time

input_path = r"P:\13. Employee Folder\Christo\control 4\Q3 25\input excel"

if __name__ == "__main__":
    print("🚀 Starting program...")
    start = time.time()
    main(input_path)
    print(f"\n✅ Program selesai dalam {time.time() - start:.2f} detik")