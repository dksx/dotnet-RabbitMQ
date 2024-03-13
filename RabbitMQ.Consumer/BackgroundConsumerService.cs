using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace RabbitMQ.Consumer;

public class BackgroundConsumerService : BackgroundService
{
    private readonly IConfigurationRoot _config;
    private readonly IHostApplicationLifetime _lifeTime;
    private readonly ILogger<BackgroundConsumerService> _logger;
    private readonly ushort _batchSize;
    private readonly string _queueName;
    private readonly ConcurrentQueue<EnqueuedMessage<dynamic>> _concurrentMessageQueue;

    private ConnectionFactory _connectionFactory;
    private IConnection _connection;
    private IModel _channel;

    public BackgroundConsumerService(IConfiguration config, IHostApplicationLifetime lifeTime, ILogger<BackgroundConsumerService> logger)
    {
        _config = (IConfigurationRoot)config;
        _logger = logger;
        _lifeTime = lifeTime;
        _concurrentMessageQueue = new ConcurrentQueue<EnqueuedMessage<dynamic>>();
        _batchSize = _config.GetValue<ushort>("RabbitMQ:BatchSize", 1);
        _queueName = _config.GetValue<string>("RabbitMQ:QueueName", "rmq");
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _connectionFactory = new ConnectionFactory();
        _config.GetSection("RabbitMQ:ConnectionFactory").Bind(_connectionFactory);
        _connection = _connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.QueueDeclarePassive(_queueName);
        _channel.BasicQos(0, _batchSize, false);
        _logger.LogInformation("Binding on Queue '{queueName}' and waiting for messages.", _queueName);

        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {

        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        DateTime lastProcessing = DateTime.UtcNow;
        AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(_channel);
        var _ = Task.Run(async () => await BatchProcessInterval(cancellationToken), cancellationToken);

        consumer.Received += async (model, ea) =>
        {
            try
            {
                _logger.LogInformation("Main task - Enqueueing message: '{DeliveryTag}'", ea.DeliveryTag);
                EnqueuedMessage<dynamic> enqueuedMessage = new EnqueuedMessage<dynamic>
                {
                    BasicProperties = ea.BasicProperties,
                    ConsumerTag = ea.ConsumerTag,
                    DeliveryTag = ea.DeliveryTag,
                    Exchange = ea.Exchange,
                    Redelivered = ea.Redelivered,
                    RoutingKey = ea.RoutingKey,
                    Body = Encoding.UTF8.GetString(ea.Body.ToArray())
                };
                _concurrentMessageQueue.Enqueue(enqueuedMessage);

                if (_concurrentMessageQueue.Count >= _batchSize)
                {
                    List<EnqueuedMessage<dynamic>> dequeuedMessages = new List<EnqueuedMessage<dynamic>>();
                    while (_concurrentMessageQueue.TryDequeue(out var message))
                    {
                        _logger.LogInformation("Main task - Dequeuing message: '{DeliveryTag}'", message.DeliveryTag);
                        dequeuedMessages.Add(message);
                    }
                    await ProcessMessages(dequeuedMessages, cancellationToken);

                    lastProcessing = DateTime.UtcNow;
                }
            }
            catch (AlreadyClosedException)
            {
                _logger.LogError("RabbitMQ is closed!");
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "An error occured when receiving, enqueueing or dequeueing the message '{DeliveryTag}'", ea.DeliveryTag);
            }
        };
        _channel.BasicConsume(queue: _queueName, autoAck: false, consumer: consumer);
        await Task.CompletedTask;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _connection?.Close();
        _logger.LogInformation("Disposed RabbitMQ connection resources");
        await base.StopAsync(cancellationToken);
    }

    private async Task BatchProcessInterval(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(120), cancellationToken);
            _logger.LogInformation("Background task - resuming from sleep and starting message processing");
            var dequeuedMessages = new List<EnqueuedMessage<dynamic>>();
            while (_concurrentMessageQueue.TryDequeue(out var message))
            {
                _logger.LogDebug("Background task - Dequeuing msg: '{DeliveryTag}'", message.DeliveryTag);
                dequeuedMessages.Add(message);
            }

            await ProcessMessages(dequeuedMessages, cancellationToken);
        }
    }

    private async Task ProcessMessages(IEnumerable<EnqueuedMessage<dynamic>> messages, CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            try
            {
                _logger.LogInformation("Background task - Processing msg: '{DeliveryTag}'", message.DeliveryTag);
                _logger.LogInformation("Message Type: '{Type}'", message.BasicProperties.Type);

                _logger.LogInformation("Body : '{messageBody}'", (string)message.Body);
                _channel.BasicAck(message.DeliveryTag, false);
                await Task.Delay(200, cancellationToken);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Error processing message '{DeliveryTag}'", message.DeliveryTag);
                _channel.BasicNack(message.DeliveryTag, false, true);
            }
        }
    }
}
