using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Newtonsoft.Json;


namespace RabbitMQ.Producer;

public class HostedRabbitMQproducer : IHostedService
{
    private readonly IConfigurationRoot _config;
    private readonly ILogger _logger;
    private IModel _channel;
    private string _queueName;

    public HostedRabbitMQproducer(IConfiguration config, ILogger<HostedRabbitMQproducer> logger)
    {
        _config = (IConfigurationRoot)config;
        _logger = logger;
    }

    private void Init()
    {
        var factory = new ConnectionFactory();
        _config.GetSection("RabbitMQ:ConnectionFactory").Bind(factory);
        _queueName = _config.GetValue<string>("RabbitMQ:QueueName", "rmq");
        IConnection connection = factory.CreateConnection();
        _channel = connection.CreateModel();
        _channel.ExchangeDeclare(exchange: "amq.direct", type: "direct", true);
        /*IDictionary<string, object> args = new Dictionary<string, object>()
        {
            { "x-max-in-memory-bytes", 4294965097 },
            { "x-max-in-memory-length", 1000000 },
            { "x-queue-type", "quorum" }
        };*/
        _channel.QueueDeclare(queue: _queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
        _channel.QueueBind(queue: _queueName, exchange: "amq.direct", routingKey: _queueName);
        _logger.LogInformation("Connection initialized");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            Init();
            bool abort;
            DateTime transferTimeout = DateTime.UtcNow.AddSeconds(10);
            do
            {
                object payload = new { Message = $"{Guid.NewGuid():N} hello world" };
                abort = DateTime.UtcNow > transferTimeout;
                PublishMessage(JsonConvert.SerializeObject(payload));
                await Task.Delay(TimeSpan.FromSeconds(0.4), cancellationToken);
            } while (!abort && !cancellationToken.IsCancellationRequested);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error initiating connection to RabbitMQ");
        }
        await Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _channel?.Close();
        _channel?.Dispose();
        await Task.CompletedTask;
    }

    private IBasicProperties CreateProperties()
    {
        IBasicProperties properties = _channel.CreateBasicProperties();
        DateTime dt = DateTime.UtcNow;
        long unixTime = ((DateTimeOffset)dt).ToUnixTimeSeconds();
        properties.Timestamp = new AmqpTimestamp(unixTime);
        properties.MessageId = Guid.NewGuid().ToString("N");
        properties.Headers = new Dictionary<string, object>()
        {
            {"Content-type", "application/json"},
        };
        properties.ContentType = "application/json";
        properties.DeliveryMode = 2;
        properties.Priority = (byte)new Random().Next(10);
        properties.Type = "Custom type";
        return properties;
    }

    private void PublishMessage(string message)
    {
        IBasicProperties properties = CreateProperties();
        byte[] body = Encoding.UTF8.GetBytes(message);
        _channel.BasicPublish(exchange: "amq.direct", routingKey: _queueName, basicProperties: properties, body: body);
        _logger.LogInformation("Pushed message - {message}", message);
    }

}