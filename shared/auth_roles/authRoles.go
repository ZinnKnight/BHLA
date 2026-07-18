package auth_roles

type Plan string

const (
	Free  Plan = "free"
	Pro   Plan = "pro"
	Admin Plan = "admin"
)

func (p Plan) String() string { return string(p) }

func All() []Plan { return []Plan{Free, Pro, Admin} }
