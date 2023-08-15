package controllers

// Controller is an interface which should only contains a method Run
type Controller interface {
	// Run starts run a controller
	Run(int, <-chan struct{})
}
