package main

func selectDb(c *Client, id int) error {
	if id < 0 || id >= server.dbnum {
		return C_ERR
	}

	c.db = server.db[id]
	return C_OK
}
