package naming

import "testing"

func TestValidQueueName(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{"orders", true},
		{"orders.v2-worker_1", true},
		{"", false},
		{"orders.dlq", false},
		{"orders/dlq", false},
		{"orders dlq", false},
		{string(make([]byte, 129)), false},
	}

	for _, tc := range tests {
		if got := ValidQueueName(tc.name); got != tc.valid {
			t.Errorf("ValidQueueName(%q) = %v, want %v", tc.name, got, tc.valid)
		}
	}
}
