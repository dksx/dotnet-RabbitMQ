using System;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;


namespace RabbitMQ.Producer;

public class RabbitMQproducer
{
    private readonly IConfigurationRoot _config;
    private readonly ILogger _logger;
    private IModel _channel;
    private string _queueName;

    public RabbitMQproducer(ILogger logger, IConfiguration configuration, string queueName)
    {
        _logger = logger;
        _queueName = queueName;
        _config = (IConfigurationRoot)configuration;
    }

    public void InitializeConnection()
    {
        try
        {
            var factory = new ConnectionFactory();
            _config.GetSection("RabbitMQ:ConnectionFactory").Bind(factory);
            var connection = factory.CreateConnection();
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
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error initiating connection to RabbitMQ");
        }
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

    public void PublishMessage(string message)
    {
        IBasicProperties properties = CreateProperties();
        var body = Encoding.UTF8.GetBytes(message);
        _channel.BasicPublish(exchange: "amq.direct", routingKey: _queueName, basicProperties: properties, body: body);
        _logger.LogInformation("Pushed message - {message}", message);
    }

    public void ShutDown()
    {
        _channel?.Close();
        _channel?.Dispose();
    }
}