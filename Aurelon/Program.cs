using Aurelon.Db;
using Aurelon.Services;
using Aurelon.Workers;
using Aurelon.Cores;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();
builder.Services.AddHealthChecks();

// Verify connection string exists early
var defaultConnection = builder.Configuration.GetConnectionString("DefaultConnection");
if (string.IsNullOrWhiteSpace(defaultConnection))
{
    throw new InvalidOperationException("Critical configuration is missing: ConnectionStrings:DefaultConnection");
}

// Aurelon Infrastructure
builder.Services.AddAurelonInfrastructure(builder.Configuration);

// Add workers
builder.Services.AddHostedService<DatasetImportWorker>();
builder.Services.AddHostedService<DocumentIngestionWorker>();
builder.Services.AddHostedService<MlTrainingWorker>();
builder.Services.AddScoped<ITaskTrainer, TabularAutoMlTrainer>();
builder.Services.AddSingleton<DatabaseInitializer>();
builder.Services.AddHostedService<DatabaseInitializationHostedService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapControllers();
app.MapHealthChecks("/health");

app.Run();