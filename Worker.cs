using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;

namespace Tronloop.NodeOrchestrator;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;

    private const string NodeId = "A0";

    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Task canListenTask = Task.CompletedTask;
        Task canSendTask = Task.CompletedTask;
        CanIsoTpListener? canListener = null;

        try
        {
            var canInterface = _configuration["Can:Interface"] ?? "can0";
            var canRxId = ParseCanId(_configuration["Can:RxId"]);
            var canTxId = ParseCanId(_configuration["Can:TxId"]);

            canListener = new CanIsoTpListener(canInterface, canRxId, canTxId, _logger);
            canListener.Open();
            canListenTask = canListener.ListenAsync(stoppingToken);
            canSendTask = SendDummyCanMessagesAsync(canListener, stoppingToken);

            _logger.LogInformation(
                "CAN ISO-TP listener started on {Interface} rx=0x{RxId:X} tx=0x{TxId:X}",
                canInterface,
                canRxId,
                canTxId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start CAN ISO-TP listener");
        }

        var factory = new MqttClientFactory();
        var client = factory.CreateMqttClient();

        client.ApplicationMessageReceivedAsync += async e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

            _logger.LogInformation("MQTT RX Topic={Topic} Payload={Payload}", topic, payload);

            try
            {
                var command = JsonSerializer.Deserialize<NodeCommand>(payload, _jsonOptions);

                if (command is null)
                {
                    await PublishAck(client, "unknown", false, "Invalid command payload", stoppingToken);
                    return;
                }

                await HandleCommand(client, command, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Command processing failed");

                await PublishAck(
                    client,
                    "unknown",
                    false,
                    ex.Message,
                    stoppingToken);
            }
        };

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("mqtt.tronloop-lab.com", 1883)
            .WithClientId($"orchestrator-{NodeId}")
            .Build();

        try
        {
            await client.ConnectAsync(options, stoppingToken);

            _logger.LogInformation("MQTT Connected");
            _logger.LogInformation("This is the new version CAN");

            await client.SubscribeAsync($"tronloop/node/{NodeId}/cmd", cancellationToken: stoppingToken);
            await client.SubscribeAsync("tronloop/broadcast/cmd", cancellationToken: stoppingToken);

            await PublishStatus(client, "online", stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                await PublishHeartbeat(client, stoppingToken);
                await Task.Delay(5000, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Worker stopping");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Worker failed");
        }
        finally
        {
            try
            {
                if (client.IsConnected)
                {
                    await PublishStatus(client, "offline", CancellationToken.None);
                    await client.DisconnectAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed while disconnecting MQTT client");
            }

            canListener?.Dispose();

            try
            {
                await canListenTask;
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "CAN listener stopped with error");
            }

            try
            {
                await canSendTask;
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "CAN sender stopped with error");
            }
        }
    }

    private async Task SendDummyCanMessagesAsync(CanIsoTpListener canListener, CancellationToken cancellationToken)
    {
        uint counter = 0;
        _logger.LogWarning("Trying to send dummy ISO-TP message");
        while (!cancellationToken.IsCancellationRequested)
        {
            var payload = new byte[12];
            BitConverter.GetBytes(counter).CopyTo(payload, 0);
            counter++;

            try
            {
                canListener.Send(payload);
                _logger.LogWarning("Sent dummy ISO-TP message");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to send dummy ISO-TP message");
            }

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }
    }

    private static uint ParseCanId(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return 0;
        }

        var trimmed = value.Trim();

        return trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
            ? uint.Parse(trimmed[2..], NumberStyles.HexNumber, CultureInfo.InvariantCulture)
            : uint.Parse(trimmed, CultureInfo.InvariantCulture);
    }

    private async Task HandleCommand(
        IMqttClient client,
        NodeCommand command,
        CancellationToken cancellationToken)
    {
        switch (command.Type)
        {
            case "start_charge":
                _logger.LogInformation("Start charge requested ");
                await PublishAck(client, command.Id, true, "Charge started", cancellationToken);
                break;

            case "stop_charge":
                _logger.LogInformation("Stop charge requested");
                await PublishAck(client, command.Id, true, "Charge stopped", cancellationToken);
                break;

            case "set_current":
                _logger.LogInformation("Set current requested: {Value}", command.Value);
                await PublishAck(client, command.Id, true, $"Current set to {command.Value}", cancellationToken);
                break;

            case "ping":
                await PublishAck(client, command.Id, true, "pong", cancellationToken);
                break;

            default:
                await PublishAck(client, command.Id, false, $"Unknown command: {command.Type}", cancellationToken);
                break;
        }
    }

    private static async Task PublishAck(
        IMqttClient client,
        string commandId,
        bool success,
        string message,
        CancellationToken cancellationToken)
    {
        var ack = new CommandAck
        {
            CommandId = commandId,
            Success = success,
            Message = message,
            TimestampUtc = DateTimeOffset.UtcNow
        };

        var mqttMessage = new MqttApplicationMessageBuilder()
            .WithTopic($"tronloop/orchestrator/{NodeId}/ack")
            .WithPayload(JsonSerializer.Serialize(ack))
            .Build();

        await client.PublishAsync(mqttMessage, cancellationToken);
    }

    private static async Task PublishStatus(
        IMqttClient client,
        string state,
        CancellationToken cancellationToken)
    {
        var status = new NodeStatus
        {
            NodeId = NodeId,
            State = state,
            TimestampUtc = DateTimeOffset.UtcNow
        };

        var message = new MqttApplicationMessageBuilder()
            .WithTopic($"tronloop/orchestrator/{NodeId}/status")
            .WithPayload(JsonSerializer.Serialize(status))
            .Build();

        await client.PublishAsync(message, cancellationToken);
    }

    private static async Task PublishHeartbeat(
        IMqttClient client,
        CancellationToken cancellationToken)
    {
        var heartbeat = new NodeStatus
        {
            NodeId = NodeId,
            State = "alive",
            TimestampUtc = DateTimeOffset.UtcNow
        };

        var message = new MqttApplicationMessageBuilder()
            .WithTopic($"tronloop/orchestrator/{NodeId}/heartbeat")
            .WithPayload(JsonSerializer.Serialize(heartbeat))
            .Build();

        await client.PublishAsync(message, cancellationToken);
    }
}

public sealed class NodeCommand
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string Type { get; set; } = "";
    public double? Value { get; set; }
}

public sealed class CommandAck
{
    public string CommandId { get; set; } = "";
    public bool Success { get; set; }
    public string Message { get; set; } = "";
    public DateTimeOffset TimestampUtc { get; set; }
}

public sealed class NodeStatus
{
    public string NodeId { get; set; } = "";
    public string State { get; set; } = "";
    public DateTimeOffset TimestampUtc { get; set; }
}