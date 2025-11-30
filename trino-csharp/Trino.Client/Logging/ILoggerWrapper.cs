using System;
using Microsoft.Extensions.Logging;

namespace Trino.Client.Logging
{
    /// <summary>
    /// Simple logger interface wrapper for Trino client logging.
    /// </summary>
    public interface ILoggerWrapper
    {
        /// <summary>
        /// Logs a message at the specified level.
        /// </summary>
        void Log(LogLevel level, EventId eventId, Exception exception, string message, params object[] args);

        /// <summary>
        /// Checks if the given log level is enabled.
        /// </summary>
        bool IsEnabled(LogLevel level);
    }

    /// <summary>
    /// Adapter that wraps an ILogger to implement ILoggerWrapper.
    /// </summary>
    public class LoggerWrapperAdapter : ILoggerWrapper
    {
        private readonly ILogger _logger;

        public LoggerWrapperAdapter(ILogger logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void Log(LogLevel level, EventId eventId, Exception exception, string message, params object[] args)
        {
            if (_logger.IsEnabled(level))
            {
                string formattedMessage = args != null && args.Length > 0
                    ? string.Format(message, args)
                    : message;
                _logger.Log(level, eventId, exception, formattedMessage);
            }
        }

        public bool IsEnabled(LogLevel level) => _logger.IsEnabled(level);
    }
}
