package main

type clusterNode struct {
}

func getClusterConnectionsCount() int {
	if server.clusterEnabled {
		return 0
	} else {
		return 0
	}
}

func nodeIsMaster(n *clusterNode) bool {
	return true
}
