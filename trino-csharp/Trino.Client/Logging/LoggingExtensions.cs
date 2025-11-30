using System;
using Microsoft.Extensions.Logging;

namespace Trino.Client.Logging
{
    /// <summary>
    /// Extension methods for ILoggerWrapper that provide formatted logging.
    /// </summary>
    public static class LoggerExtensions
    {
        /// <summary>Formats and writes a debug log message.</summary>
        public static void LogDebug(this ILoggerWrapper logger, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Debug, 0, null, message, args);
        }

        /// <summary>Formats and writes a debug log message.</summary>
        public static void LogDebug(this ILoggerWrapper logger, EventId eventId, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Debug, eventId, null, message, args);
        }

        /// <summary>Formats and writes a debug log message.</summary>
        public static void LogDebug(this ILoggerWrapper logger, EventId eventId, Exception exception, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Debug, eventId, exception, message, args);
        }

        /// <summary>Formats and writes a trace log message.</summary>
        public static void LogTrace(this ILoggerWrapper logger, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Trace, 0, null, message, args);
        }

        /// <summary>Formats and writes a trace log message.</summary>
        public static void LogTrace(this ILoggerWrapper logger, EventId eventId, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Trace, eventId, null, message, args);
        }

        /// <summary>Formats and writes a trace log message.</summary>
        public static void LogTrace(this ILoggerWrapper logger, EventId eventId, Exception exception, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Trace, eventId, exception, message, args);
        }

        /// <summary>Formats and writes an informational log message.</summary>
        public static void LogInformation(this ILoggerWrapper logger, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Information, 0, null, message, args);
        }

        /// <summary>Formats and writes an informational log message.</summary>
        public static void LogInformation(this ILoggerWrapper logger, EventId eventId, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Information, eventId, null, message, args);
        }

        /// <summary>Formats and writes an informational log message.</summary>
        public static void LogInformation(this ILoggerWrapper logger, EventId eventId, Exception exception, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Information, eventId, exception, message, args);
        }

        /// <summary>Formats and writes a warning log message.</summary>
        public static void LogWarning(this ILoggerWrapper logger, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Warning, 0, null, message, args);
        }

        /// <summary>Formats and writes a warning log message.</summary>
        public static void LogWarning(this ILoggerWrapper logger, EventId eventId, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Warning, eventId, null, message, args);
        }

        /// <summary>Formats and writes a warning log message.</summary>
        public static void LogWarning(this ILoggerWrapper logger, EventId eventId, Exception exception, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Warning, eventId, exception, message, args);
        }

        /// <summary>Formats and writes an error log message.</summary>
        public static void LogError(this ILoggerWrapper logger, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Error, 0, null, message, args);
        }

        /// <summary>Formats and writes an error log message.</summary>
        public static void LogError(this ILoggerWrapper logger, EventId eventId, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Error, eventId, null, message, args);
        }

        /// <summary>Formats and writes an error log message.</summary>
        public static void LogError(this ILoggerWrapper logger, EventId eventId, Exception exception, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Error, eventId, exception, message, args);
        }

        /// <summary>Formats and writes a critical log message.</summary>
        public static void LogCritical(this ILoggerWrapper logger, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Critical, 0, null, message, args);
        }

        /// <summary>Formats and writes a critical log message.</summary>
        public static void LogCritical(this ILoggerWrapper logger, EventId eventId, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Critical, eventId, null, message, args);
        }

        /// <summary>Formats and writes a critical log message.</summary>
        public static void LogCritical(this ILoggerWrapper logger, EventId eventId, Exception exception, string message, params object[] args)
        {
            if (logger == null) return;
            logger.Log(LogLevel.Critical, eventId, exception, message, args);
        }
    }
}
