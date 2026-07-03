using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;

namespace Tronloop.NodeOrchestrator;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    private const string NodeId = "A0";

    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new MqttClientFactory();
        var client = factory.CreateMqttClient();

        client.ApplicationMessageReceivedAsync += async e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

            _logger.LogInformation("RX Topic={Topic} Payload={Payload}", topic, payload);

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
                    stoppingToken
                );
            }
        };

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("mqtt.tronloop-lab.com", 1883)
            .WithClientId($"orchestrator-{NodeId}")
            .Build();


        await client.ConnectAsync(options, stoppingToken);

        _logger.LogInformation("MQTT Connected");
        _logger.LogInformation("This is the new version 3");

        await client.SubscribeAsync($"tronloop/node/{NodeId}/cmd", cancellationToken: stoppingToken);
        await client.SubscribeAsync("tronloop/broadcast/cmd", cancellationToken: stoppingToken);

        await PublishStatus(client, "online", stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            await PublishHeartbeat(client, stoppingToken);
            await Task.Delay(5000, stoppingToken);
        }

        await PublishStatus(client, "offline", CancellationToken.None);
        await client.DisconnectAsync();
    }

    private async Task HandleCommand(
        IMqttClient client,
        NodeCommand command,
        CancellationToken cancellationToken)
    {
        switch (command.Type)
        {
            case "start_charge":
                _logger.LogInformation("Start charge requested");
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