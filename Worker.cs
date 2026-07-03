using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;

namespace Tronloop.NodeOrchestrator;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new MqttClientFactory();

        var client = factory.CreateMqttClient();

        client.ApplicationMessageReceivedAsync += e =>
        {
            _logger.LogInformation(
                "RX Topic={Topic} Payload={Payload}",
                e.ApplicationMessage.Topic,
                Encoding.UTF8.GetString(e.ApplicationMessage.Payload)
            );

            return Task.CompletedTask;
        };

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", 1883)
            .WithClientId("gateway-rack01")
            .Build();

        await client.ConnectAsync(options, stoppingToken);

        _logger.LogInformation("MQTT Connected");

        await client.SubscribeAsync("tronloop/#", cancellationToken: stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            var message = new MqttApplicationMessageBuilder()
                .WithTopic("tronloop/test")
                .WithPayload("Hello MQTT")
                .Build();

            await client.PublishAsync(message, stoppingToken);

            await Task.Delay(5000, stoppingToken);
        }
    }
}