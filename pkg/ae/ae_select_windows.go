package ae

type apiState struct {

}

// todo windows select ???
func apiCreate(el *AeEventLoop) error {
	state := new(apiState)


	el.ApiData = state
	return nil
}
