package broker

// Middleware wraps a HandlerFunc. The chain is composed left-to-right:
// the first argument is the outermost wrapper.
type Middleware func(next HandlerFunc) HandlerFunc

// chain composes a list of middleware around a final HandlerFunc. An
// empty list returns h unchanged.
func chain(mw []Middleware, h HandlerFunc) HandlerFunc {
	// Compose right-to-left so mw[0] ends up outermost.
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}
