package model

// UnwrapItem splits a dump item into document id / type / source.
//
// Dump items produced by the es read path look like
// {"_id":..., "_index":..., "_type":..., "_source":{...}}; plain NDJSON
// lines (without a _source key) are treated as the document source itself.
// wrapped reports whether the item was a dump-style wrapped item.
func UnwrapItem(item map[string]any) (id string, typ string, src map[string]any, wrapped bool) {
	if raw, ok := item["_source"]; ok {
		if m, ok := raw.(map[string]any); ok {
			id, _ = item["_id"].(string)
			typ, _ = item["_type"].(string)
			return id, typ, m, true
		}
	}

	return "", "", item, false
}
