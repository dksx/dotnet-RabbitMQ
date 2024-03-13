using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;


namespace RabbitMQ.Producer;

class Program
{
    static void Main(string[] args)
    {
        /*await Host.CreateDefaultBuilder(args)
        .ConfigureAppConfiguration((context, config) =>
        {
            IHostEnvironment env = context.HostingEnvironment;
            config.AddEnvironmentVariables()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true, reloadOnChange: true)
                .AddCommandLine(args);
        })
        .ConfigureServices(services =>
        {
            //services.AddSingleton<IHostedService, HostedRabbitMQproducer>();
        })
        .Build()
        .RunAsync();*/

        IHost host = Host.CreateDefaultBuilder(args).Build();
        IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();
        ILogger logger = host.Services.GetRequiredService<ILogger<RabbitMQproducer>>();
        logger.LogInformation("Host created.");

        IConfiguration config = host.Services.GetRequiredService<IConfiguration>();

        string queueName = config.GetValue<string>("RabbitMQ:QueueName", "rmq");

        uint msgLimit = 10;
        RabbitMQproducer producer = new RabbitMQproducer(logger, configuration, queueName);

        try
        {
            uint msgCount = 1;
            producer.InitializeConnection();
            do
            {
                object payload = new { Message = $"{Guid.NewGuid():N} hello world" };
                producer.PublishMessage(JsonConvert.SerializeObject(payload));
                msgCount++;
            } while (msgCount <= msgLimit);
        }
        catch (Exception exc)
        {
            logger.LogError(exc, "Error sending message or initializing producer");
        }
        producer?.ShutDown();
    }
}

