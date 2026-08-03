package model

// immutable index settings which must not be sent to PUT /_settings
var immutableSettings = map[string]struct{}{
	"uuid":                     {},
	"version":                  {},
	"creation_date":            {},
	"provided_name":            {},
	"number_of_shards":         {},
	"number_of_routing_shards": {},
}

// FilterUpdatableSettings extracts updatable settings from a raw GetSettings
// response ({index: {settings: {index: {...}}}}), dropping immutable keys.
func FilterUpdatableSettings(setting map[string]any) map[string]any {
	payload := make(map[string]any)

	for _, idx := range setting {
		settings, ok := idx.(map[string]any)
		if !ok {
			continue
		}

		raw, ok := settings["settings"].(map[string]any)
		if !ok {
			continue
		}

		for k, v := range raw {
			if k == "index" {
				if m, ok := v.(map[string]any); ok {
					for sk, sv := range m {
						if _, immutable := immutableSettings[sk]; !immutable {
							payload[sk] = sv
						}
					}
				}
				continue
			}
			payload[k] = v
		}
	}

	return payload
}
