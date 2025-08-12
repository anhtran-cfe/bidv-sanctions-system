import pymupdf4llm
import subprocess
import sys

def main():
    try:
        md_text = pymupdf4llm.to_markdown("202500395.pdf")
        # print("Kết quả markdown:")
        # print(md_text)
        # Ghi ra file output.md
        with open("output.md", "w", encoding="utf-8") as f:
            f.write(md_text)
        print("Đã ghi kết quả ra file output.md")
    except Exception as e:
        print(f"Lỗi khi chuyển PDF: {e}")

if __name__ == "__main__":
    main()
