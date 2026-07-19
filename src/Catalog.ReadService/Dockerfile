FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["src/Shared.Contracts/Shared.Contracts.csproj", "src/Shared.Contracts/"]
COPY ["src/Catalog.ReadService/Catalog.ReadService.csproj", "src/Catalog.ReadService/"]
RUN dotnet restore "src/Catalog.ReadService/Catalog.ReadService.csproj"
COPY . .
RUN dotnet publish "src/Catalog.ReadService/Catalog.ReadService.csproj" -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Catalog.ReadService.dll"]
