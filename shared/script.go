package shared

type Script struct {
	ID          string `gorethink:"id"`
	Interpreter string `gorethink:"interpreter"`
	Source      string `gorethink:"source"`
}
