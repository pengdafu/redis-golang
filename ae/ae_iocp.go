//go:build windows

package ae

type aeApiState struct {
}

// todo call what?
func aeApiCreate(el *EventLoop) error {
	state := new(aeApiState)

	el.ApiData = state
	return nil
}
