package main

// todo
func dbASyncDelete(db *redisDb, key *robj) bool {
	return dbSyncDelete(db, key)
}
