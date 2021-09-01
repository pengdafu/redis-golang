package ae

type apiState struct {

}

// todo windows select ???
func apiCreate(el *eventLoop) error {
	state := new(apiState)


	el.ApiData = state
	return nil
}
