package deliver

import "testing"

// TestFanout_MultiTarget_HappyPath is the project's first integration contract:
// one inbound POST fans out to every configured target, and each target receives
// the original body unchanged. It is unskipped once the fan-out pipeline lands.
func TestFanout_MultiTarget_HappyPath(t *testing.T) {
	t.Skip("delivery pipeline not implemented yet")
}
