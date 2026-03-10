using Amazon.Runtime;
using Amazon.S3;
using Aurelon.Db;
using Aurelon.Options;
using Aurelon.Repositories.Datasets;
using Aurelon.Repositories.Documents;
using Aurelon.Repositories.Jobs;
using Aurelon.Repositories.Outbox;
using Aurelon.Repositories.Training;
using Aurelon.Repositories.Uploads;
using Aurelon.Storage;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Aurelon.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAurelonInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' was not found.");

        services.Configure<CloudflareR2Options>(configuration.GetSection("CloudflareR2"));

        services.AddSingleton(sp => NpgsqlDataSource.Create(connectionString));
        services.AddSingleton<IAppDbConnectionFactory, AppDbConnectionFactory>();

        services.AddDbContext<ApplicationIdentityDbContext>(options =>
            options.UseNpgsql(connectionString));

        services.AddSingleton<IAmazonS3>(sp =>
        {
            var options = configuration.GetSection("CloudflareR2").Get<CloudflareR2Options>()
                ?? throw new InvalidOperationException("CloudflareR2 configuration is missing.");
            var credentials = new BasicAWSCredentials(options.AccessKey, options.SecretKey);
            var config = new AmazonS3Config
            {
                ServiceURL = options.ServiceUrl,
                ForcePathStyle = true,
            };
            return new AmazonS3Client(credentials, config);
        });

        services.AddScoped<IUploadRepository, UploadRepository>();
        services.AddScoped<IDocumentJobRepository, DocumentJobRepository>();
        services.AddScoped<IDocumentLibraryRepository, DocumentLibraryRepository>();
        services.AddScoped<IConversationRepository, ConversationRepository>();
        services.AddScoped<IDatasetRepository, DatasetRepository>();
        services.AddScoped<IDatasetVersionRepository, DatasetVersionRepository>();
        services.AddScoped<IDatasetJobRepository, DatasetJobRepository>();
        services.AddScoped<IDatasetQueryRepository, DatasetQueryRepository>();
        services.AddScoped<ITrainingJobRepository, TrainingJobRepository>();
        services.AddScoped<IFeatureSnapshotRepository, FeatureSnapshotRepository>();
        services.AddScoped<IModelRegistryRepository, ModelRegistryRepository>();
        services.AddScoped<IJobReadRepository, JobReadRepository>();
        services.AddScoped<IOutboxRepository, OutboxRepository>();
        services.AddSingleton<IR2ObjectStorage, R2ObjectStorage>();

        return services;
    }
}
