package pkg

type aeApiState struct {
}

// todo windows select ???
func aeApiCreate(el *AeEventLoop) error {
	state := new(aeApiState)

	el.ApiData = state
	return nil
}
