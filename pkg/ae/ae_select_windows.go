package ae

import "syscall"

type apiState struct {

}

func apiCreate(el *eventLoop) error {
	state := new(apiState)

	f, _ := syscall.Socket()


	el.ApiData = state
	return nil
}
