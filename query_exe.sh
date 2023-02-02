sparql-to-json $1 > query.json
python3 main.py
rm -f query.json