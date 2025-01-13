package main

import (
	"fmt"
	"strings"
)

func ParseTags(tags string) (map[string]string, error) {
	retv := make(map[string]string)
	for _, tag := range strings.Split(tags, ",") {
		parts := strings.Split(tag, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid tag: %s", tag)
		}
		retv[parts[0]] = parts[1]
	}
	return retv, nil
}

func PartialWhereClauseForTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	var parts []string
	for k, v := range tags {
		parts = append(parts, fmt.Sprintf(`%s='%s'`, k, v))
	}
	return " AND " + strings.Join(parts, " AND ")
}
