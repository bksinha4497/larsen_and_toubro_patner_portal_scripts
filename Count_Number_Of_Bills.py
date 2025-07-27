import os


def count_pdfs_in_bills_folders(parent_folder):
    """
    Recursively counts the total number of .pdf files within all subfolders
    named "Bills" inside a given parent folder.

    Args:
        parent_folder (str): The path to the parent folder (e.g., "qa").

    Returns:
        int: The total count of .pdf files in "Bills" folders.
    """
    total_pdf_count = 0

    # Walk through the directory tree
    for root, dirs, files in os.walk(parent_folder):
        # Check if the current directory is named "Bills"
        if os.path.basename(root) == "Bills":
            # If it is, count the PDF files in it
            for file in files:
                if file.lower().endswith(".pdf"):
                    total_pdf_count += 1
    return total_pdf_count


if __name__ == "__main__":
    qa_folder_path = '/Users/kumar/Desktop/LNT_Partner_Downloads'  # Assuming your main folder is named "qa"

    if not os.path.exists(qa_folder_path):
        print(f"Error: The folder '{qa_folder_path}' does not exist.")
    else:
        pdf_count = count_pdfs_in_bills_folders(qa_folder_path)
        print(f"Total number of .pdf files in all 'Bills' folders within '{qa_folder_path}': {pdf_count}")