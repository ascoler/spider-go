package main
import (
	"github.com/temoto/robotstxt"
	
	"log/slog"
)


func getRobotsTxt(url string) (*robotstxt.Group, error) {
	robots, err := robotstxt.FromString(url)
	if err != nil {
		slog.Error("Failed to parse robots.txt", "Error", err)
		return nil, err
	}
	groups := robots.FindGroup("*")

	return groups, nil
}

func CheckAlowed(url string) (bool, error) {
	group, err := getRobotsTxt(url)
	if err != nil {
		return false, err
	}

	allowed := group.Test(url)
	return allowed, nil
}

