import csv


def writeFileMethod(inputKeywords, csvFilePath):
    data = [["INVENTION_TITLE", "ABSTRACT_OF_INVENTION", "COMPLETE_SPECIFICATION"],
            [inputKeywords, inputKeywords, inputKeywords]]

    # Writing to CSV file
    with open(csvFilePath, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write data to the CSV file
        for row in data:
            writer.writerow(row)

    print(f"CSV file '{csvFilePath}' has been created.")
