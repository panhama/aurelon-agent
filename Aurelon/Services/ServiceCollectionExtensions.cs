using Amazon.Runtime;
using Amazon.S3;
using Aurelon.Db;
using Aurelon.Models;
using Aurelon.Repositories;
using Aurelon.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.VectorData;
using Microsoft.SemanticKernel.Connectors.Qdrant;
using Npgsql;
using Qdrant.Client;

namespace Aurelon.Services;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAurelonInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("DefaultConnection string is required.");

        services.Configure<CloudflareR2Options>(configuration.GetSection("CloudflareR2"));
        
        services.AddSingleton(sp => NpgsqlDataSource.Create(connectionString));
        services.AddSingleton<IAppDbConnectionFactory, AppDbConnectionFactory>();

        var r2Options = configuration.GetSection("CloudflareR2").Get<CloudflareR2Options>() ?? new CloudflareR2Options();
        var awsOptions = configuration.GetAWSOptions();
        awsOptions.Credentials = new BasicAWSCredentials(r2Options.AccessKey, r2Options.SecretKey);
        services.AddDefaultAWSOptions(awsOptions);
        services.AddAWSService<IAmazonS3>();

        services.AddSingleton<IR2ObjectStorage, R2ObjectStorage>();
        services.AddScoped<DocumentWorkRepository>();
        services.AddScoped<DatasetWorkRepository>();
        
        // Extractors
        services.AddSingleton<PdfExtractor>();
        services.AddSingleton<DocxExtractor>();
        services.AddSingleton<MarkdownExtractor>();
        services.AddSingleton<ExcelExtractor>();
        
        // RAG Services
        services.AddSingleton<ITokenEstimator, VoyageSafeTokenEstimator>();
        services.AddSingleton<LayoutAwareChunker>();
        
        services.AddHttpClient<VoyageEmbeddingClient>((sp, client) => {
             // Configure client if needed
        });

        services.AddScoped<DocumentExtractionService>();
        services.AddScoped<DocumentIndexingService>();
        services.AddScoped<RetrievalService>();
        
        services.Configure<ClickHouseOptions>(configuration.GetSection("ClickHouse"));
        services.AddHttpClient();
        
        // Training Services
        services.AddScoped<DatasetParsingService>();
        services.AddScoped<ClickHouseDatasetLoader>();
        services.AddScoped<ClickHouseTrainingDataService>();
        services.AddScoped<DatasetManifestBuilder>();
        services.AddScoped<R2ArtifactService>();

        // Vector Store
        services.AddSingleton<QdrantClient>(sp => {
            var endpoint = configuration["Qdrant:Endpoint"] ?? "http://localhost:6334";
            var apiKey = configuration["Qdrant:ApiKey"];
            var uri = new Uri(endpoint);
            return new QdrantClient(uri.Host, uri.Port, uri.Scheme == "https", apiKey);
        });
        
        services.AddQdrantVectorStore();

        return services;
    }
}
