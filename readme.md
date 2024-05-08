# esgo2dump
# dump elasticsearch with golang

---

- 当前仅支持 elasticsearch 7

---

### install

- with golang >= 1.18

  `go install github.com/loveuer/esgo2dump@latest`

- download pre-build release:

  [releases](https://github.com/loveuer/esgo2dump/releases)

### usage

`esgo2dump -h`

```bash
esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=http://192.168.1.1:9200/some_index --limit=5000

esgo2dump --input=https://username:password@127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --source='id;name;age;address;phones' --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query='{"match": {"name": "some_name"}}'

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query_file=my_queries.json
```

- example_queries.json
```json
{"bool":{"should":[{"term":{"user_id":{"value":"123"}}},{"term":{"user_id":{"value":"456"}}}]}}
{"bool":{"should":[{"term":{"user_id":{"value":"abc"}}},{"term":{"user_id":{"value":"def"}}}]}}
{"bool":{"should":[{"term":{"user_id":{"value":"ABC"}}},{"term":{"user_id":{"value":"DEF"}}}]}}
```

### roadmap

- [x] data dump
- [x] mapping dump
- [x] es to file
- [x] es to es
- [x] auto create index with mapping
- [ ] auto create index with mapping,setting
- [ ] support es8
- [ ] support es6