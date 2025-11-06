# esgo2dump
# dump elasticsearch with golang

![Test](https://github.com/loveuer/esgo2dump/workflows/Test/badge.svg)
![Go Version](https://img.shields.io/badge/Go-%3E%3D1.18-blue.svg)
![License](https://img.shields.io/github/license/loveuer/esgo2dump)

---

- 支持 elasticsearch 7, elasticsearch 6

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

esgo2dump --input=http://127.0.0.1:9200/some_index --i-version 6 --output=./data.json

esgo2dump --output=http://127.0.0.1:9200/some_index --o-version 6 --input=./data.json

esgo2dump --input=https://username:password@127.0.0.1:9200/some_index --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --source='id;name;age;address;phones' --output=./data.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query='{"match": {"name": "some_name"}}'

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query_file=my_queries.json

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./output_dir --split-limit=1000
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
- [x] support es6
- [ ] [Feature Request #1](https://github.com/loveuer/esgo2dump/issues/1): Supports more than 10,000 lines of query_file
- [x] args: split-limit (auto split json output file)
- [ ] auto create index with mapping,setting
- [ ] support es8
