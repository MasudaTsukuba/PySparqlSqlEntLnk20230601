import csv


class Output:  # write the sparql query results into a file
    def __init__(self):
        pass

    # 結果の表示, output.csvに出力される
    @staticmethod
    def save_file(output, results, headers):
        output = output.replace('.txt', '.csv')
        sorted_results = results
        index = 0
        results2 = []
        for item in results:
            if not isinstance(item[0], str):
                print(index, item)
                results2.append(['None']+item[1:])
            else:
                results2.append(item)
            index += 1
        sorted_results = sorted(results2, key=lambda x: x[0])  # sort
        # try:
        #     sorted_results = sorted(results, key=lambda x: x[0])  # sort
        # except:
        #     pass
        with open(output, mode='w') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(sorted_results)
