package instrument

import (
	"os"

	"github.com/rs/zerolog"
)

// ZerologAdapter wraps zerolog to implement Temporal's log.Logger interface.
type ZerologAdapter struct {
	logger zerolog.Logger
}

// NewZerologAdapter creates a new instance of ZerologAdapter with proper configuration.
func NewZerologAdapter() *ZerologAdapter {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Create logger with proper configuration
	zerologLogger := zerolog.New(os.Stdout).
		With().
		Timestamp().
		Caller().
		Logger()

	return &ZerologAdapter{logger: zerologLogger}
}

// Debug logs debug messages.
func (zl *ZerologAdapter) Debug(msg string, keyvals ...interface{}) {
	zl.logger.Debug().Fields(convertKeyValsToMap(keyvals)).Msg(msg)
}

// Info logs info messages.
func (zl *ZerologAdapter) Info(msg string, keyvals ...interface{}) {
	zl.logger.Info().Fields(convertKeyValsToMap(keyvals)).Msg(msg)
}

// Warn logs warning messages.
func (zl *ZerologAdapter) Warn(msg string, keyvals ...interface{}) {
	zl.logger.Warn().Fields(convertKeyValsToMap(keyvals)).Msg(msg)
}

// Error logs error messages.
func (zl *ZerologAdapter) Error(msg string, keyvals ...interface{}) {
	zl.logger.Error().Fields(convertKeyValsToMap(keyvals)).Msg(msg)
}

// Helper function to convert key-value pairs into a map for structured logging.
func convertKeyValsToMap(keyvals []interface{}) map[string]interface{} {
	fields := make(map[string]interface{})
	for i := 0; i < len(keyvals)-1; i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			continue
		}
		fields[key] = keyvals[i+1]
	}
	return fields
}
