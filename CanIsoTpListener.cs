using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Tronloop.NodeOrchestrator;

public sealed class CanIsoTpListener : IDisposable
{
    private const int AF_CAN = 29;
    private const int SOCK_DGRAM = 2;
    private const int CAN_ISOTP = 6;
    private const int SOL_SOCKET = 1;
    private const int SO_RCVTIMEO = 20;

    private const int EAGAIN = 11;
    private const int EWOULDBLOCK = 11;
    private const int EBADF = 9;
    private const int ENETDOWN = 100;
    private const int ENODEV = 19;
    private const int ETIMEDOUT = 110;

    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(2);

    private readonly string _interfaceName;
    private readonly uint _rxId;
    private readonly uint _txId;
    private readonly ILogger _logger;
    private readonly object _sync = new();

    private int _socketFd = -1;
    private bool _disposed;

    public CanIsoTpListener(string interfaceName, uint rxId, uint txId, ILogger logger)
    {
        _interfaceName = interfaceName;
        _rxId = rxId;
        _txId = txId;
        _logger = logger;
    }

    public void Open()
    {
        lock (_sync)
        {
            ThrowIfDisposed();

            if (_socketFd >= 0)
            {
                return;
            }

            _socketFd = OpenSocket();
        }
    }

    public async Task ListenAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                EnsureOpen();

                var fd = GetSocketFd();
                var bytesRead = read(fd, buffer, buffer.Length);

                if (bytesRead > 0)
                {
                    var hex = Convert.ToHexString(buffer, 0, (int)bytesRead);
                    Console.WriteLine($"[ISO-TP] {DateTime.Now:HH:mm:ss.fff} <- {hex}");
                    _logger.LogInformation("ISO-TP RX [{Length} bytes]: {Hex}", bytesRead, hex);
                    continue;
                }

                if (bytesRead < 0)
                {
                    var errno = Marshal.GetLastWin32Error();

                    if (errno is EAGAIN or EWOULDBLOCK or ETIMEDOUT)
                    {
                        continue;
                    }

                    _logger.LogWarning("ISO-TP read error (errno={Errno}); reconnecting socket.", errno);

                    CloseSocketSafely();

                    if (IsFatalSocketError(errno))
                    {
                        await DelayBeforeReconnect(cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "ISO-TP listener failed; will retry.");
                CloseSocketSafely();
                await DelayBeforeReconnect(cancellationToken);
            }
        }
    }

    private void EnsureOpen()
    {
        lock (_sync)
        {
            ThrowIfDisposed();

            if (_socketFd >= 0)
            {
                return;
            }

            _socketFd = OpenSocket();
        }
    }

    private int OpenSocket()
    {
        var ifIndex = if_nametoindex(_interfaceName);
        if (ifIndex == 0)
        {
            throw new InvalidOperationException(
                $"CAN interface '{_interfaceName}' not found or not available (errno={Marshal.GetLastWin32Error()}).");
        }

        var fd = socket(AF_CAN, SOCK_DGRAM, CAN_ISOTP);
        if (fd < 0)
        {
            throw new InvalidOperationException(
                $"Failed to create ISO-TP socket (errno={Marshal.GetLastWin32Error()}).");
        }

        try
        {
            var timeout = new Timeval { Seconds = 1, Microseconds = 0 };
            if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, ref timeout, Marshal.SizeOf<Timeval>()) != 0)
            {
                _logger.LogWarning(
                    "Failed to set ISO-TP receive timeout (errno={Errno})",
                    Marshal.GetLastWin32Error());
            }

            var addr = new SockaddrCan
            {
                CanFamily = AF_CAN,
                CanIfIndex = (int)ifIndex,
                RxId = _rxId,
                TxId = _txId
            };

            if (bind(fd, ref addr, Marshal.SizeOf<SockaddrCan>()) != 0)
            {
                var errno = Marshal.GetLastWin32Error();
                throw new InvalidOperationException(
                    $"Failed to bind ISO-TP socket on '{_interfaceName}' " +
                    $"(rx=0x{_rxId:X}, tx=0x{_txId:X}, errno={errno}).");
            }

            _logger.LogInformation(
                "ISO-TP socket bound on {Interface} rx=0x{RxId:X} tx=0x{TxId:X}",
                _interfaceName, _rxId, _txId);

            return fd;
        }
        catch
        {
            close(fd);
            throw;
        }
    }

    private int GetSocketFd()
    {
        lock (_sync)
        {
            return _socketFd;
        }
    }

    private void CloseSocketSafely()
    {
        lock (_sync)
        {
            if (_socketFd < 0)
            {
                return;
            }

            var fd = _socketFd;
            _socketFd = -1;
            close(fd);
        }
    }

    private static bool IsFatalSocketError(int errno)
    {
        return errno is EBADF or ENETDOWN or ENODEV;
    }

    private static async Task DelayBeforeReconnect(CancellationToken cancellationToken)
    {
        await Task.Delay(ReconnectDelay, cancellationToken);
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            if (_socketFd >= 0)
            {
                close(_socketFd);
                _socketFd = -1;
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(CanIsoTpListener));
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct SockaddrCan
    {
        public ushort CanFamily;
        public int CanIfIndex;
        public uint RxId;
        public uint TxId;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Timeval
    {
        public long Seconds;
        public long Microseconds;
    }

    [DllImport("libc", SetLastError = true)]
    private static extern uint if_nametoindex(string ifname);

    [DllImport("libc", SetLastError = true)]
    private static extern int socket(int domain, int type, int protocol);

    [DllImport("libc", SetLastError = true)]
    private static extern int bind(int sockfd, ref SockaddrCan addr, int addrlen);

    [DllImport("libc", SetLastError = true)]
    private static extern int setsockopt(int sockfd, int level, int optname, ref Timeval optval, int optlen);

    [DllImport("libc", SetLastError = true)]
    private static extern nint read(int fd, byte[] buf, nint count);

    [DllImport("libc", SetLastError = true)]
    private static extern int close(int fd);
}