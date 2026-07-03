using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Tronloop.NodeOrchestrator;

/// <summary>
/// Listens on a Linux SocketCAN ISO-TP (ISO 15765-2) socket and prints received payloads.
/// Requires the "can-isotp" kernel module and a CAN interface (e.g. can0/vcan0) already up.
/// </summary>
public sealed class CanIsoTpListener : IDisposable
{
    private const int AF_CAN = 29;
    private const int SOCK_DGRAM = 2;
    private const int CAN_ISOTP = 6;
    private const int SOL_SOCKET = 1;
    private const int SO_RCVTIMEO = 20;
    private const int EAGAIN = 11;
    private const int EWOULDBLOCK = 11;
    private const int ETIMEDOUT = 110;

    private readonly string _interfaceName;
    private readonly uint _rxId;
    private readonly uint _txId;
    private readonly ILogger _logger;
    private int _socketFd = -1;

    public CanIsoTpListener(string interfaceName, uint rxId, uint txId, ILogger logger)
    {
        _interfaceName = interfaceName;
        _rxId = rxId;
        _txId = txId;
        _logger = logger;
    }

    public void Open()
    {
        var ifIndex = if_nametoindex(_interfaceName);
        if (ifIndex == 0)
        {
            throw new InvalidOperationException(
                $"CAN interface '{_interfaceName}' not found (errno={Marshal.GetLastWin32Error()}).");
        }

        _socketFd = socket(AF_CAN, SOCK_DGRAM, CAN_ISOTP);
        if (_socketFd < 0)
        {
            throw new InvalidOperationException(
                $"Failed to create ISO-TP socket (errno={Marshal.GetLastWin32Error()}).");
        }

        // Bound recv timeout so ListenAsync can observe cancellation without blocking forever.
        var timeout = new Timeval { Seconds = 1, Microseconds = 0 };
        setsockopt(_socketFd, SOL_SOCKET, SO_RCVTIMEO, ref timeout, Marshal.SizeOf<Timeval>());

        var addr = new SockaddrCan
        {
            CanFamily = AF_CAN,
            CanIfIndex = (int)ifIndex,
            RxId = _rxId,
            TxId = _txId
        };

        if (bind(_socketFd, ref addr, Marshal.SizeOf<SockaddrCan>()) != 0)
        {
            var errno = Marshal.GetLastWin32Error();
            close(_socketFd);
            _socketFd = -1;
            throw new InvalidOperationException(
                $"Failed to bind ISO-TP socket on '{_interfaceName}' (rx=0x{_rxId:X}, tx=0x{_txId:X}, errno={errno}).");
        }

        _logger.LogInformation(
            "ISO-TP socket bound on {Interface} rx=0x{RxId:X} tx=0x{TxId:X}",
            _interfaceName, _rxId, _txId);
    }

    public Task ListenAsync(CancellationToken cancellationToken)
    {
        if (_socketFd < 0)
        {
            throw new InvalidOperationException("Socket not open. Call Open() first.");
        }

        return Task.Run(() =>
        {
            var buffer = new byte[4096];

            while (!cancellationToken.IsCancellationRequested)
            {
                var bytesRead = read(_socketFd, buffer, buffer.Length);

                if (bytesRead > 0)
                {
                    var hex = Convert.ToHexString(buffer, 0, (int)bytesRead);
                    Console.WriteLine($"[ISO-TP] {DateTime.Now:HH:mm:ss.fff} <- {hex}");
                    _logger.LogInformation("ISO-TP RX [{Length} bytes]: {Hex}", bytesRead, hex);
                }
                else if (bytesRead < 0)
                {
                    var errno = Marshal.GetLastWin32Error();
                    if (errno is EAGAIN or EWOULDBLOCK or ETIMEDOUT)
                    {
                        continue; // recv timeout, loop back to re-check cancellation
                    }

                    _logger.LogWarning("ISO-TP read error (errno={Errno})", errno);
                }
            }
        }, cancellationToken);
    }

    public void Dispose()
    {
        if (_socketFd >= 0)
        {
            close(_socketFd);
            _socketFd = -1;
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
