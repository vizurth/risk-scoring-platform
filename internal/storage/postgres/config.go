package postgres

// Config содержит настройки для подключения к Postgres
type Config struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	MaxConns int32  `yaml:"max_conns" env:"MAX_CONNS" env-default:"10"`
	MinConns int32  `yaml:"min_conns" env:"MIN_CONNS" env-default:"5"`
}
