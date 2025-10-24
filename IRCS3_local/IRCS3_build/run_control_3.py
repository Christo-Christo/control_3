from Syntax.main import main
import sys
import os


input_sheet_path = r"D:\Run Control 3\Input Sheet_IRCS3.xlsx"







if __name__ == "__main__":
    success = main(input_sheet_path)
    if os.name == 'nt':
        input("\nPress Enter to exit...")
    sys.exit(0 if success else 1)