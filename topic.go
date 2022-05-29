package pubsub

// Topic identifies a particular Topic on which messages can be published.
type Topic string

// String returns the string representation of the Topic.
func (t Topic) String() string {
	return string(t)
}
