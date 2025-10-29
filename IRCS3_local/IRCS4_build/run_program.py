from syntax.main import main
import time
import datetime

input_path = r"P:\13. Employee Folder\Christo\control 4\Q3 25\input excel\input trad con.xlsx"

if __name__ == "__main__":
    print("🚀 Starting program...")
    start = time.time()
    main(input_path)
    elapsed = time.time() - start
    formatted = str(datetime.timedelta(seconds=int(elapsed)))
    print(f"\n⏱️ Total runtime: {formatted}")