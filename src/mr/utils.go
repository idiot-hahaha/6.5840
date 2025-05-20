package mr

import "fmt"

func mapOutFilePath(taskID int64, reduceIndex int64) string {
	path := fmt.Sprintf("mr-%d-%d", taskID, reduceIndex)
	return path
}
