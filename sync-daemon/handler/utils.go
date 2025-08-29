package handler

import (
	"encoding/json"
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
)

// ExtractStringArray extracts an array of strings from JSON event data
func ExtractStringArray(eventBuf []byte, path ...string) ([]string, error) {
	var result []string
	
	// First try to get as array
	arrayData, dataType, _, err := jsonparser.Get(eventBuf, path...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to extract %v", path)
	}

	switch dataType {
	case jsonparser.Array:
		// Parse as array
		err = jsonparser.ArrayEach(arrayData, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			if err != nil {
				return
			}
			if dataType == jsonparser.String {
				str, _ := jsonparser.ParseString(value)
				result = append(result, str)
			}
		})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse array for %v", path)
		}
	case jsonparser.String:
		// Single string value
		str, err := jsonparser.ParseString(arrayData)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse string for %v", path)
		}
		result = append(result, str)
	default:
		return nil, fmt.Errorf("unexpected data type for %v: %v", path, dataType)
	}

	return result, nil
}
