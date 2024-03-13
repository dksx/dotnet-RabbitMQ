using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace RabbitMQ.Consumer;

class Program
{
    public static IConfigurationRoot Configuration { get; set; }
    static async Task Main(string[] args)
    {
        // Application code should start here.
        await Host.CreateDefaultBuilder(args)
        .ConfigureAppConfiguration((context, config) =>
        {
            IHostEnvironment env = context.HostingEnvironment;
            config.AddEnvironmentVariables()
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true, reloadOnChange: true)
                .AddCommandLine(args);
            Configuration = config.Build();
        })
        .ConfigureServices(services =>
        {
            services.AddHostedService<BackgroundConsumerService>();
        })
        .Build()
        .RunAsync();
    }
}

