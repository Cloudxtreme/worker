package shared

type Job struct {
	ID          string      `gorethink:"id" json:"id"`
	When        string      `gorethink:"when" json:"when"`
	Description string      `gorethink:"description" json:"description"`
	Name        string      `gorethink:"name" json:"name"`
	Args        interface{} `gorethink:"args" json:"args"`
}
