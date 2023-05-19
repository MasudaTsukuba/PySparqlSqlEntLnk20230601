sparql-to-json $1 > query.json
cp ./data_set2/URI/URI_data.db ./data_set2/URI/URI_data_copy.db
python3 main.py
rm -f query.json
mv ./data_set2/URI/URI_data_copy.db ./data_set2/URI/URI_data.db