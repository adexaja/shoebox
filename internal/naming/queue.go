// Package naming contains validation rules for queue names shared by the
// public API and the standalone HTTP API.
package naming

import "strings"

// ValidQueueName reports whether name is a valid user-visible queue name.
// Names ending in .dlq are reserved for Shoebox dead-letter queues and must
// only be accessed through the DLQ manager/API operations.
func ValidQueueName(name string) bool {
	if name == "" || len(name) > 128 || strings.HasSuffix(name, ".dlq") {
		return false
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') &&
			(r < '0' || r > '9') && r != '.' && r != '-' && r != '_' {
			return false
		}
	}
	return true
}
