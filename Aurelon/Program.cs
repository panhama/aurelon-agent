using Aurelon.Db;
using Aurelon.Services;
using Aurelon.Workers;
using Aurelon.Cores;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddOpenApi();

// Aurelon Infrastructure
builder.Services.AddAurelonInfrastructure(builder.Configuration);

// Add workers
builder.Services.AddHostedService<DatasetImportWorker>();
builder.Services.AddHostedService<DocumentIngestionWorker>();
builder.Services.AddHostedService<MlTrainingWorker>();
builder.Services.AddScoped<ITaskTrainer, TabularAutoMlTrainer>();
builder.Services.AddSingleton<DatabaseInitializer>();

var app = builder.Build();

// Run DB Initializer
using (var scope = app.Services.CreateScope())
{
    var dbInit = scope.ServiceProvider.GetRequiredService<DatabaseInitializer>();
    await dbInit.InitializeAsync();
}

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapControllers();

app.Run();