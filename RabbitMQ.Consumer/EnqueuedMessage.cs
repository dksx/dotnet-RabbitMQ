using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;


namespace RabbitMQ.Consumer;

public class EnqueuedMessage<T>
{
    public IBasicProperties BasicProperties { get; set; }
    public string ConsumerTag { get; set; }
    public ulong DeliveryTag { get; set; }
    public string Exchange { get; set; }
    public bool Redelivered { get; set; }
    public string RoutingKey { get; set; }
    public T Body { get; set; }
}
