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

esgo2dump --input=http://127.0.0.1:9200/some_index --output=./data.json --query='{"match": {"name": "some_name"}}'`,
```

### roadmap

- [x] data dump
- [x] mapping dump
- [x] es to file
- [x] es to es
- [x] auto create index with mapping
- [ ] auto create index with mapping,setting
- [ ] support es8