package ae

type apiState struct {

}

// todo windows select ???
func apiCreate(el *EventLoop) error {
	state := new(apiState)


	el.ApiData = state
	return nil
}
