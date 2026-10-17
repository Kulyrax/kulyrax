FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/AutomotiveCsvProducer/AutomotiveCsvProducer.csproj ./AutomotiveCsvProducer/
RUN dotnet restore ./AutomotiveCsvProducer/AutomotiveCsvProducer.csproj
COPY src/AutomotiveCsvProducer/ ./AutomotiveCsvProducer/
RUN dotnet publish ./AutomotiveCsvProducer/AutomotiveCsvProducer.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "AutomotiveCsvProducer.dll"]
