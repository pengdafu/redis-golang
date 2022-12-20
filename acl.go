package main

var DefaultUser *user

func init() {
	DefaultUser = new(user)
	DefaultUser.flags |= USER_FLAG_NOPASS
}
