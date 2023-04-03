import csv


class Output:  # write the sparql query results into a file
    def __init__(self):
        pass

    # 結果の表示, output.csvに出力される
    @staticmethod
    def save_file(output, results, headers):
        with open(output, mode='w') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(results)
